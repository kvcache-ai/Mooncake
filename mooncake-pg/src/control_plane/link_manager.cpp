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
    LOG(INFO) << "LinkManager::init() called, rank=" << rank
              << " max_world_size=" << max_world_size
              << " engine=" << (void*)engine;
    // If already initialized, return.  Also reject init after shutdown
    // (re-init-after-shutdown is not supported  - create a new process).
    if (initialized_.exchange(true, std::memory_order_acq_rel)) {
        LOG(WARNING) << "LinkManager::init() already initialized, returning";
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

    if (!skip_warmup_) {
        warmup_send_region_ = std::make_unique<int32_t[]>(kMaxNumRanks);
        std::memset(warmup_send_region_.get(), 0,
                    kMaxNumRanks * sizeof(int32_t));
        warmup_send_region_[0] = 1;
        int rc = engine_->registerLocalMemory(warmup_send_region_.get(),
                                              kMaxNumRanks * sizeof(int32_t),
                                              kWildcardLocation);
        if (rc != 0) {
            warmup_send_region_.reset();
            LOG(ERROR) << "LinkManager: failed to register warmup send region";
        }

        warmup_recv_region_ = std::make_unique<int32_t[]>(kMaxNumRanks);
        std::memset(warmup_recv_region_.get(), 0,
                    kMaxNumRanks * sizeof(int32_t));
        LOG(INFO) << "LinkManager: registering warmup_recv_region addr="
                  << (void*)warmup_recv_region_.get()
                  << " length=" << kMaxNumRanks * sizeof(int32_t);
        rc = engine_->registerLocalMemory(warmup_recv_region_.get(),
                                          kMaxNumRanks * sizeof(int32_t),
                                          kWildcardLocation);
        if (rc != 0) {
            LOG(ERROR)
                << "LinkManager: failed to register warmup recv region, rc="
                << rc;
            warmup_recv_region_.reset();
        } else {
            LOG(INFO) << "LinkManager: warmup_recv_region registered OK, "
                      << "getWarmupRecvAddr=" << (void*)getWarmupRecvAddr();
        }
    }

    // Bootstrap self: open local segment, mark CONNECTED.
    TransferMetadata::SegmentID self_target_id{};
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto& link = peers_[rank_];
        link.state = PeerLinkState::CONNECTED;
        link.candidate = false;
        link.target_id = engine_->openSegment(local_server_name_);
        self_target_id = link.target_id.value();
    }

    // Write read model for self.
    publishLinkUp(rank_, self_target_id);
    read_state_[rank_].rank_state.store(
        static_cast<uint8_t>(RankState::HEALTHY), std::memory_order_release);

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
        link.state = PeerLinkState::IDLE;
        link.candidate = false;
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
    if (peer < 0 || peer >= max_world_size_) return;

    LOG(INFO) << "LinkManager: enablePeerProbe rank " << peer
              << " server_name=" << server_name
              << " warmup_recv_addr=" << (void*)warmup_recv_addr
              << " skip_warmup=" << skip_warmup_;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto& link = peers_[peer];
    link.server_name = server_name;
    link.warmup_recv_addr = warmup_recv_addr;
    link.candidate = true;
    link.skip_warmup = skip_warmup_;

    // Reset probe backoff so connection attempt starts promptly.
    link.probe_backoff = PeerLink::kProbeBackoffMin;
    link.next_probe_time = std::chrono::steady_clock::now();

    wakeup();
}

void LinkManager::disconnect(GlobalRank peer) {
    if (peer == rank_) return;
    if (peer < 0 || peer >= max_world_size_) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    tearDownPeerLink(peer, /*stop_reconnect=*/false);
}

void LinkManager::stopReconnect(GlobalRank peer) {
    if (peer == rank_) return;
    if (peer < 0 || peer >= max_world_size_) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    peers_[peer].candidate = false;
}

bool LinkManager::isConnected(GlobalRank peer) const {
    if (peer < 0 || peer >= max_world_size_) return false;
    return read_state_[peer].link_connected.load(std::memory_order_acquire) !=
           0;
}

void LinkManager::setEventCallback(EventCallback callback) {
    std::lock_guard<std::mutex> lock(event_callback_mutex_);
    event_callback_ = std::move(callback);
}

void LinkManager::setRankStates(const std::vector<uint8_t>& states) {
    for (int i = 0; i < max_world_size_ && i < static_cast<int>(states.size());
         ++i) {
        read_state_[i].rank_state.store(states[i], std::memory_order_release);
    }
}

std::optional<PeerReadHandle> LinkManager::resolvePeer(GlobalRank peer) const {
    if (peer < 0 || peer >= max_world_size_) return std::nullopt;

    // Use a version counter to detect torn reads: if the link goes down and
    // back up between reading link_connected and target_id, we could see a
    // stale target_id.  Comparing versions before and after catches this.
    auto& rs = read_state_[peer];

    uint64_t v1 = rs.version.load(std::memory_order_acquire);
    if (rs.link_connected.load(std::memory_order_acquire) == 0)
        return std::nullopt;
    PeerReadHandle handle;
    handle.target_id = rs.target_id.load(std::memory_order_acquire);
    uint64_t v2 = rs.version.load(std::memory_order_acquire);

    // If version changed, the link state was modified during our read  -
    // target_id may be from a different LinkUp cycle.  Treat as
    // disconnected.
    if (v1 != v2) return std::nullopt;

    return handle;
}

bool LinkManager::isRankReady(GlobalRank peer) const {
    if (peer < 0 || peer >= max_world_size_) return false;
    return read_state_[peer].rank_state.load(std::memory_order_acquire) ==
               static_cast<uint8_t>(RankState::HEALTHY) &&
           read_state_[peer].link_connected.load(std::memory_order_acquire) !=
               0;
}

void LinkManager::publishLinkUp(GlobalRank peer,
                                TransferMetadata::SegmentID target_id) {
    if (peer < 0 || peer >= max_world_size_) return;
    // Order: target_id before link_connected before version increment.
    // resolvePeer uses the version counter to detect torn reads.
    read_state_[peer].target_id.store(target_id, std::memory_order_relaxed);
    read_state_[peer].link_connected.store(1, std::memory_order_release);
    read_state_[peer].version.fetch_add(1, std::memory_order_release);
}

void LinkManager::publishLinkDown(GlobalRank peer) {
    if (peer < 0 || peer >= max_world_size_) return;
    read_state_[peer].link_connected.store(0, std::memory_order_release);
    read_state_[peer].version.fetch_add(1, std::memory_order_release);
}

void LinkManager::tearDownPeerLink(GlobalRank peer, bool stop_reconnect) {
    auto& link = peers_[peer];

    if (link.target_id.has_value()) {
        engine_->closeSegment(link.target_id.value());
        link.target_id = std::nullopt;
    }

    if (link.warmup_batch_id.has_value()) {
        engine_->freeBatchID(link.warmup_batch_id.value());
        link.warmup_batch_id = std::nullopt;
    }

    // Clear the warmup handshake signal so the next probe doesn't see a
    // stale completion flag from a prior connection cycle.
    if (warmup_recv_region_) warmup_recv_region_[peer] = 0;

    link.state = PeerLinkState::IDLE;
    if (stop_reconnect) {
        link.candidate = false;
    }

    // Immediately clear the worker read model so worker threads stop
    // constructing new transfer requests to this peer.  The LinkDown
    // event callback may also call publishLinkDown, but that is
    // idempotent (atomic store 0).
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
                if (!link.candidate) continue;
                if (link.state == PeerLinkState::CONNECTED) continue;
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
        case PeerLinkState::IDLE: {
            if (link.server_name.empty()) return false;

            auto segment_id = engine_->openSegment(link.server_name);
            // openSegment returns (SegmentHandle)(-1) on failure (UINT64_MAX),
            // which is truthy so a plain !segment_id check would miss it.
            if (segment_id == static_cast<SegmentHandle>(-1)) {
                // openSegment failed  - backoff and retry.
                link.probe_backoff = std::min(link.probe_backoff * 2,
                                              PeerLink::kProbeBackoffMax);
                return false;
            }

            link.target_id = segment_id;

            LOG(INFO) << "LinkManager: probePeer rank " << peer
                      << " openSegment OK, segment_id=" << segment_id
                      << " server_name=" << link.server_name
                      << " warmup_recv_addr=" << (void*)link.warmup_recv_addr
                      << " skip_warmup=" << link.skip_warmup
                      << " will_initiate_warmup=" << (peer <= rank_);

            if (link.skip_warmup) {
                // MNNVL fabric: connectivity is guaranteed, skip warmup
                // handshake.
                link.state = PeerLinkState::CONNECTED;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, segment_id);
                emit(TELinkEvent{.kind = TELinkEvent::Kind::LinkUp,
                                 .peer = peer,
                                 .target_id = segment_id});
                return true;
            }

            // Warmup handshake based on total ordering to avoid both sides
            // initiating warmup simultaneously.
            if (peer <= rank_) {
                // We initiate: write to peer's warmup recv region.
                auto batch_id = engine_->allocateBatchID(1);
                uint64_t target_offset =
                    link.warmup_recv_addr + rank_ * sizeof(int32_t);
                LOG(INFO) << "LinkManager: warmup write rank " << rank_
                          << " -> " << peer << " segment_id=" << segment_id
                          << " warmup_recv_addr="
                          << (void*)link.warmup_recv_addr
                          << " target_offset=" << (void*)target_offset
                          << " source=" << (void*)warmup_send_region_.get();
                engine_->submitTransfer(batch_id,
                                        {TransferRequest{
                                            .opcode = TransferRequest::WRITE,
                                            .source = warmup_send_region_.get(),
                                            .target_id = segment_id,
                                            .target_offset = target_offset,
                                            .length = sizeof(int32_t),
                                        }});
                link.warmup_batch_id = batch_id;
                link.state = PeerLinkState::WAITING_WARMUP_TRANSFER;
            } else {
                // Wait for the peer to warmup us.
                link.state = PeerLinkState::WAITING_PEER_WARMUP;
            }
            return true;
        }

        case PeerLinkState::WAITING_WARMUP_TRANSFER: {
            if (!link.warmup_batch_id.has_value()) {
                link.state = PeerLinkState::IDLE;
                return false;
            }

            TransferStatus status;
            engine_->getTransferStatus(link.warmup_batch_id.value(), 0, status);

            if (status.s == TransferStatusEnum::COMPLETED) {
                LOG(INFO) << "LinkManager: warmup rank " << rank_ << " -> "
                          << peer << " COMPLETED";
                engine_->freeBatchID(link.warmup_batch_id.value());
                link.warmup_batch_id = std::nullopt;
                link.state = PeerLinkState::CONNECTED;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, link.target_id.value());
                emit(TELinkEvent{.kind = TELinkEvent::Kind::LinkUp,
                                 .peer = peer,
                                 .target_id = link.target_id});
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
                link.state = PeerLinkState::IDLE;
                link.probe_backoff = std::min(link.probe_backoff * 2,
                                              PeerLink::kProbeBackoffMax);
                return false;
            }
            // Still in flight  - don't advance backoff.
            return false;
        }

        case PeerLinkState::WAITING_PEER_WARMUP: {
            if (!warmup_recv_region_) {
                link.state = PeerLinkState::IDLE;
                return false;
            }

            if (*reinterpret_cast<volatile int32_t*>(
                    &warmup_recv_region_[peer])) {
                link.state = PeerLinkState::CONNECTED;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, link.target_id.value());
                emit(TELinkEvent{.kind = TELinkEvent::Kind::LinkUp,
                                 .peer = peer,
                                 .target_id = link.target_id});
                return true;
            }
            // Still waiting  - don't advance backoff.
            return false;
        }

        case PeerLinkState::CONNECTED:
        case PeerLinkState::EXPIRING:
            break;
    }

    return false;
}

}  // namespace mooncake
