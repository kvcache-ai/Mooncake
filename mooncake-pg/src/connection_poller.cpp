#include <c10/util/Exception.h>
#include <connection_poller.h>
#include <ATen/cuda/CUDAContext.h>
#include <cuda.h>
#include <cuda_alike.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <algorithm>
#include <cstring>
#include <limits>
#include "memory_location.h"
#include "mooncake_worker.cuh"

namespace mooncake {

// Same check as nvlink_transport.cpp and mooncake_ep_buffer.cpp.
// On MNNVL clusters all GPUs support fabric mem handles, meaning
// NVLink transport can only access cuMemCreate(FABRIC) memory
// cross-node -- CPU heap buffers are invisible to remote peers.
static bool supportFabricMem() {
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
ConnectionContext::ConnectionContext(int backendIndex, int rank, int size,
                                     bool isDummy,
                                     uint64_t* local2global_rank_map,
                                     c10::intrusive_ptr<::c10d::Store> store,
                                     std::shared_ptr<TransferGroupMeta> meta,
                                     std::shared_ptr<P2PProxy> p2p_proxy,
                                     TransferEngine* engine)
    : backendIndex_(backendIndex),
      rank_(rank),
      groupSize_(size),
      isDummy_(isDummy),
      establishedGroupSize_(0),
      local2global_rank_map_(local2global_rank_map),
      store_(std::move(store)),
      meta_(std::move(meta)),
      p2p_proxy_(std::move(p2p_proxy)),
      engine_(engine),
      skip_warmup_(supportFabricMem()) {
    if (skip_warmup_) {
        // On MNNVL clusters, CPU heap buffers aren't fabric-accessible so
        // remote NVLink writes to them will fail. The fabric topology already
        // guarantees connectivity, so we skip the warmup handshake entirely.
        warmup_send_region_ = nullptr;
        warmup_recv_region_ = nullptr;
        return;
    }

    warmup_send_region_ = new int32_t[kMaxNumRanks]{};
    warmup_send_region_[0] = 1;
    int rc = engine_->registerLocalMemory(
        warmup_send_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, "Failed to register local memory for context.");

    warmup_recv_region_ = new int32_t[kMaxNumRanks]{};
    rc = engine_->registerLocalMemory(
        warmup_recv_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, "Failed to register local memory for context.");
}

ConnectionContext::~ConnectionContext() {
    for (int i = 0; i < groupSize_; ++i) {
        if (peerStates_[i].segmentId.has_value()) {
            engine_->closeSegment(peerStates_[i].segmentId.value());
        }
    }

    if (warmup_send_region_) {
        engine_->unregisterLocalMemory(warmup_send_region_);
        delete[] warmup_send_region_;
    }
    if (warmup_recv_region_) {
        engine_->unregisterLocalMemory(warmup_recv_region_);
        delete[] warmup_recv_region_;
    }
}

int ConnectionContext::getTotalConnectedPeers() const {
    return totalConnectedPeers_.load(std::memory_order_acquire);
}

void ConnectionContext::extendGroupSizeTo(int newGroupSize) {
    const int oldGroupSize = groupSize_.load(std::memory_order_acquire);
    if (newGroupSize == oldGroupSize) return;

    TORCH_CHECK(
        newGroupSize >= 0 && static_cast<size_t>(newGroupSize) < kMaxNumRanks,
        "Size out of range");
    TORCH_CHECK(newGroupSize >= oldGroupSize, "newGroupSize < oldGroupSize");

    // Reset local peer state for newly added ranks
    for (int i = oldGroupSize; i < newGroupSize; ++i) {
        meta_->peerConnected[i] = false;
    }

    groupSize_.store(newGroupSize, std::memory_order_release);
}

bool ConnectionContext::isAllPeerConnected() const {
    return totalConnectedPeers_ == groupSize_;
}

void ConnectionContext::waitUntilAllConnected() {
    if (isAllPeerConnected()) return;

    std::unique_lock<std::mutex> lock(backend_wakeup_mutex_);
    backend_wakeup_cv_.wait(lock, [this]() {
        return isAllPeerConnected() ||
               isShutdown_.load(std::memory_order_acquire);
    });
    establishedGroupSize_.store(groupSize_, std::memory_order_release);
}

void ConnectionContext::waitUntilNewRanksConnected() {
    if (isDummy_) {
        return;
    }
    const int targetGroupSize = groupSize_.load(std::memory_order_acquire);
    const int established =
        establishedGroupSize_.load(std::memory_order_acquire);
    if (established >= targetGroupSize) {
        return;
    }

    std::unique_lock<std::mutex> lock(backend_wakeup_mutex_);
    backend_wakeup_cv_.wait(lock, [this, targetGroupSize, established]() {
        if (isShutdown_.load(std::memory_order_acquire)) {
            return true;
        }

        for (int i = established; i < targetGroupSize; ++i) {
            if (!meta_->peerConnected[i]) {
                return false;
            }
        }
        return true;
    });

    establishedGroupSize_.store(targetGroupSize, std::memory_order_release);
}

void ConnectionContext::bootstrapLocalPeer(const std::string& localServerName,
                                           const SegmentInfo& localRankInfo) {
    auto& peerState = peerStates_[rank_];
    if (peerState.state == PeerConnectionState::CONNECTED) {
        return;
    }

    auto segment_id = engine_->openSegment(localServerName);
    meta_->segmentIDs[rank_] = segment_id;
    peerState.segmentId = segment_id;
    memcpy(&meta_->segmentInfos[rank_], &localRankInfo, sizeof(SegmentInfo));

    meta_->peerConnected[rank_] = true;
    ConnectionPoller::GetInstance()
        .global_peerConnected_[local2global_rank_map_[rank_]] = true;
    peerState.state = PeerConnectionState::CONNECTED;

    {
        std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
        totalConnectedPeers_.store(1, std::memory_order_release);
        if (isAllPeerConnected()) {
            backend_wakeup_cv_.notify_all();
        }
    }
}

void ConnectionContext::shutdown() {
    // Notify backends that may be blocked in waitUntilAllConnected.
    {
        std::lock_guard<std::mutex> backend_lock(backend_wakeup_mutex_);
        isShutdown_.store(true, std::memory_order_release);
    }
    backend_wakeup_cv_.notify_all();
}

bool ConnectionContext::poll() {
    if (isShutdown_.load(std::memory_order_acquire)) {
        return false;
    }

    bool did_work = false;

    // Poll all peers sequentially.
    for (int pollingRank = 0; pollingRank < groupSize_; ++pollingRank) {
        did_work |= pollPeer(pollingRank);
    }

    return did_work;
}

bool ConnectionContext::pollPeer(int pollingRank) {
    auto globalPollingRank = local2global_rank_map_[pollingRank];
    auto& global_peerConnected_ =
        ConnectionPoller::GetInstance().global_peerConnected_;
    auto& peerState = peerStates_[pollingRank];
    bool state_changed = false;

    switch (peerState.state) {
        case PeerConnectionState::WAITING_STORE: {
            // See if we need backoff
            auto now = std::chrono::steady_clock::now();
            auto elapsed = static_cast<size_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - peerState.last_check_store)
                    .count());
            if (elapsed < peerState.check_store_backoff_ms) {
                return false;
            }

            peerState.last_check_store = now;

            auto serverNameKey =
                getServerNameStoreKey(backendIndex_, pollingRank);
            auto bufferKey = getBufferStoreKey(backendIndex_, pollingRank);

            std::string peerServerName;
            std::vector<uint8_t> buffer_data;
            try {
                if (!store_->check({serverNameKey, bufferKey})) {
                    peerState.increaseCheckStoreBackoff();
                    return false;
                }
                peerServerName = store_->get_to_str(serverNameKey);
                buffer_data = store_->get(bufferKey);

                if (buffer_data.size() < sizeof(SegmentInfo)) {
                    LOG(WARNING)
                        << "Rank " << rank_ << " got invalid buffer data from "
                        << pollingRank << ".";
                    peerState.increaseCheckStoreBackoff();
                    return false;
                }
            } catch (const std::exception& e) {
                peerState.increaseCheckStoreBackoff();
                return false;
            }

            auto segment_id = engine_->openSegment(peerServerName);
            meta_->segmentIDs[pollingRank] = segment_id;
            peerState.segmentId = segment_id;

            memcpy(&meta_->segmentInfos[pollingRank], buffer_data.data(),
                   sizeof(SegmentInfo));

            if (skip_warmup_) {
                // MNNVL: fabric guarantees connectivity, skip warmup write
                // since CPU heap buffers aren't fabric-accessible anyway.
                meta_->peerConnected[pollingRank] = true;
                global_peerConnected_[globalPollingRank] = true;
                peerState.state = PeerConnectionState::CONNECTED;
                {
                    std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
                    totalConnectedPeers_.fetch_add(1,
                                                   std::memory_order_release);
                    backend_wakeup_cv_.notify_all();
                }
            } else if (pollingRank <= rank_) {
                // Send a warmup request to establish connections
                auto batchID = engine_->allocateBatchID(1);
                engine_->submitTransfer(
                    batchID,
                    {TransferRequest{
                        .opcode = TransferRequest::WRITE,
                        .source = warmup_send_region_,
                        .target_id = meta_->segmentIDs[pollingRank],
                        .target_offset =
                            meta_->segmentInfos[pollingRank].warmup_buffer[1] +
                            rank_ * sizeof(int32_t),
                        .length = sizeof(int32_t),
                    }});
                peerState.warmupBatchId = batchID;
                peerState.state = PeerConnectionState::WAITING_WARMUP_TRANSFER;
            } else {
                // For pollingRank > rank_, wait for the peer's warmup write to
                // arrive.
                peerState.state = PeerConnectionState::WAITING_PEER_WARMUP;
            }
            peerState.resetCheckStoreBackoff();
            state_changed = true;
            break;
        }

        case PeerConnectionState::WAITING_WARMUP_TRANSFER: {
            TransferStatus status;
            engine_->getTransferStatus(peerState.warmupBatchId.value(), 0,
                                       status);
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(peerState.warmupBatchId.value());
                peerState.warmupBatchId = std::nullopt;
                meta_->peerConnected[pollingRank] = true;
                global_peerConnected_[globalPollingRank] = true;
                peerState.state = PeerConnectionState::CONNECTED;

                {
                    std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
                    totalConnectedPeers_.fetch_add(1,
                                                   std::memory_order_release);
                    backend_wakeup_cv_.notify_all();
                }
                state_changed = true;
            } else if (status.s == TransferStatusEnum::FAILED) {
                LOG(WARNING) << "Warmup request " << rank_ << " -> "
                             << pollingRank << " failed.";
                // Free resources and retry
                engine_->freeBatchID(peerState.warmupBatchId.value());
                engine_->closeSegment(peerState.segmentId.value());
                peerState.warmupBatchId = std::nullopt;
                peerState.segmentId = std::nullopt;
                peerState.state = PeerConnectionState::WAITING_STORE;
                state_changed = true;
            }
            break;
        }

        case PeerConnectionState::WAITING_PEER_WARMUP: {
            if (*reinterpret_cast<volatile int32_t*>(
                    &warmup_recv_region_[pollingRank])) {
                meta_->peerConnected[pollingRank] = true;
                global_peerConnected_[globalPollingRank] = true;
                peerState.state = PeerConnectionState::CONNECTED;
                {
                    std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
                    totalConnectedPeers_.fetch_add(1,
                                                   std::memory_order_release);
                    backend_wakeup_cv_.notify_all();
                }
                state_changed = true;
            }
            break;
        }

        case PeerConnectionState::CONNECTED: {
            // ATTENTION: Ensure consistency of local (meta_->peerConnected)
            //            and global (global_peerConnected_).
            //
            // Assuming there are two backends, and Backend A detects a failure.
            // (by setting meta_->peerConnected[pollingRank] to false)
            //
            // For Backend A:
            //   Observes `Local=false, Global=true` and updates `Global=false`.
            //
            // For Backend B:
            //   Observes `Local=true, Global=false` and updates `Local=false`.
            //
            // Because ConnectionPoller runs as a single thread and processes
            // contexts sequentially, no race conditions occur. Besides, the
            // failure signal is guaranteed to propagate to all other contexts
            // (e.g., Backend B) before the initiator (e.g., Backend A) is
            // processed again.
            //
            // Thus, we ensure that if one backend disconnects, all backends
            // disconnect.

            if (meta_->peerConnected[pollingRank] &&
                global_peerConnected_[globalPollingRank]) {
                // happy path: both are connected.
                break;
            }

            // If we reach here, at least one peer connected (local or global)
            // reports a failure. We must set both to false here.
            global_peerConnected_[globalPollingRank] = false;
            meta_->peerConnected[pollingRank] = false;
            meta_->activeRanks[pollingRank] = false;
            meta_->activeRanksTensor[pollingRank] = 0;

            // Reset store
            store_->deleteKey(
                getServerNameStoreKey(backendIndex_, pollingRank));
            store_->deleteKey(getBufferStoreKey(backendIndex_, pollingRank));
            store_->deleteKey(
                getExtensionTaskCountStoreKey(backendIndex_, pollingRank));
            store_->deleteKey(
                getExtensionActiveRanksStoreKey(backendIndex_, pollingRank));

            // Reset warmup region
            *reinterpret_cast<volatile int32_t*>(
                &warmup_recv_region_[pollingRank]) = 0;

            // Reset P2PProxy states
            p2p_proxy_->ResetPeerState(pollingRank);

            // Back to WAITING_STORE to reconnect it.
            peerState.state = PeerConnectionState::WAITING_STORE;
            engine_->closeSegment(peerState.segmentId.value());
            peerState.segmentId = std::nullopt;
            totalConnectedPeers_.fetch_sub(1);
            state_changed = true;
            break;
        }

        case PeerConnectionState::EXPIRING:
            TORCH_CHECK(
                false, "Unexpected PeerConnectionState::EXPIRING in pollPeer.");
            break;
    }

    return state_changed;
}

bool ConnectionContext::tryStop() {
    bool stopped = true;
    for (auto& peerState : peerStates_) {
        if (peerState.state == PeerConnectionState::WAITING_WARMUP_TRANSFER) {
            TransferStatus status;
            engine_->getTransferStatus(peerState.warmupBatchId.value(), 0,
                                       status);

            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED) {
                engine_->freeBatchID(peerState.warmupBatchId.value());
                peerState.warmupBatchId = std::nullopt;
                peerState.state = PeerConnectionState::EXPIRING;
            } else {
                stopped = false;
            }
        }
    }
    return stopped;
}

ConnectionPoller::ConnectionPoller() = default;

void ConnectionPoller::ensureThreadStarted() {
    bool expected = false;
    if (!pollerThreadStarted_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }
    pollerThread_ = std::thread([this] { pollerLoop(); });
}

void ConnectionPoller::registerContext(
    const std::shared_ptr<ConnectionContext>& ctx) {
    ensureThreadStarted();
    {
        std::lock_guard<std::mutex> lock(contexts_mutex_);
        contexts_.push_back(ctx);
        contexts_version_.fetch_add(1, std::memory_order_release);
    }
    wakeup_cv_.notify_one();
}

void ConnectionPoller::removeContext(
    const std::shared_ptr<ConnectionContext>& ctx) {
    TORCH_CHECK(ctx->isShutdown_, "connection context hasn't shutdown.");
    {
        std::lock_guard<std::mutex> lock(contexts_mutex_);
        contexts_.erase(std::remove(contexts_.begin(), contexts_.end(), ctx),
                        contexts_.end());
        contexts_version_.fetch_add(1, std::memory_order_release);
    }
    wakeup_cv_.notify_one();
}

void ConnectionPoller::pollerLoop() {
    // Similar to `local_proxies` in P2PDeviceWorker's worker loop,
    // it is a thread-local cache to avoid locking too frequently.
    std::vector<std::shared_ptr<ConnectionContext>> local_contexts;
    uint64_t local_version = std::numeric_limits<uint64_t>::max();

    // Keep track of removed contexts and free them properly to ensure
    // graceful backend shutdown.
    std::vector<std::shared_ptr<ConnectionContext>> zombie_contexts;

    while (true) {
        const auto current_version =
            contexts_version_.load(std::memory_order_acquire);
        if (local_version != current_version) {
            std::vector<std::shared_ptr<ConnectionContext>> new_contexts;
            {
                std::lock_guard<std::mutex> lock(contexts_mutex_);
                new_contexts = contexts_;
                // Reload to ensure version matches with new_contexts
                local_version =
                    contexts_version_.load(std::memory_order_acquire);
            }

            // Find zombie contexts
            for (const auto& old_c : local_contexts) {
                auto it =
                    std::find(new_contexts.begin(), new_contexts.end(), old_c);
                if (it != new_contexts.end()) continue;
                zombie_contexts.emplace_back(old_c);
            }

            local_contexts = std::move(new_contexts);
        }

        bool did_work = false;
        bool all_connected = true;

        for (const auto& ctx : local_contexts) {
            did_work |= ctx->poll();
            all_connected &= ctx->isAllPeerConnected();
        }

        for (auto it = zombie_contexts.begin(); it < zombie_contexts.end();) {
            auto& zombie = *it;
            if (zombie->tryStop())
                it = zombie_contexts.erase(it);
            else {
                ++it;
            }
        }

        if (did_work) continue;

        std::unique_lock<std::mutex> lock(wakeup_mutex_);
        auto sleep_ms = all_connected ? ALL_CONNECTED_IDLE_SLEEP_MS
                                      : CONNECTING_IDLE_SLEEP_MS;
        wakeup_cv_.wait_for(lock, std::chrono::milliseconds(sleep_ms), [&]() {
            if (local_version !=
                contexts_version_.load(std::memory_order_acquire))
                return true;
            return false;
        });
    }
}
}  // namespace mooncake
