#include <c10/util/Exception.h>
#include <connection_poller.h>
#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <algorithm>
#include <limits>
#include "mooncake_worker.cuh"

namespace mooncake {
ConnectionContext::ConnectionContext(int backendIndex, int rank, int size, uint64_t* local2global_rank_map,
                                     c10::intrusive_ptr<::c10d::Store> store,
                                     std::shared_ptr<TransferGroupMeta> meta,
                                     TransferEngine* engine)
    : backendIndex_(backendIndex),
      rank_(rank),
      size_(size),
      local2global_rank_map_(local2global_rank_map),
      store_(std::move(store)),
      meta_(std::move(meta)),
      engine_(engine) {
    warmup_send_region_ = new int32_t[kMaxNumRanks];
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
    for (int i = 0; i < size_; ++i) {
        if (peerStates_[i].segmentId.has_value()) {
            engine_->closeSegment(peerStates_[i].segmentId.value());
        }
    }

    engine_->unregisterLocalMemory(warmup_send_region_);
    engine_->unregisterLocalMemory(warmup_recv_region_);
    delete[] warmup_send_region_;
    delete[] warmup_recv_region_;
}

void ConnectionContext::waitUntilAllConnected() {
    std::unique_lock<std::mutex> lock(backend_wakeup_mutex_);
    backend_wakeup_cv_.wait(lock, [this]() {
        return isAllPeerConnected() ||
               isShutdown_.load(std::memory_order_acquire);
    });
}

void ConnectionContext::shutdown() {
    // Wake up backend if they block at waitUntilAllConnected.
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

    // Poll all peers in parallel
    for (int pollingRank = 0; pollingRank < size_; ++pollingRank) {
        if (!meta_->peerConnected[pollingRank]) {
            did_work |= pollPeer(pollingRank);
        }
    }

    return did_work;
}

bool ConnectionContext::pollPeer(int pollingRank) {
    auto global_rank = local2global_rank_map_[pollingRank];
    auto& global_peerConnected_ = ConnectionPoller::GetInstance().global_peerConnected_;
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

            std::string serverNameKey = "server_name_" +
                                        std::to_string(backendIndex_) + "_" +
                                        std::to_string(pollingRank);
            std::string bufferKey = "buffer_" + std::to_string(backendIndex_) +
                                    "_" + std::to_string(pollingRank);

            std::string peerServerName;
            std::vector<uint8_t> buffer_data;
            try {
                if (!store_->check({serverNameKey, bufferKey})) {
                    peerState.increaseCheckStoreBackoff();
                    return false;
                }
                peerServerName = store_->get_to_str(serverNameKey);
                buffer_data = store_->get(bufferKey);
            } catch (const std::exception& e) {
                peerState.increaseCheckStoreBackoff();
                return false;
            }

            auto segment_id = engine_->openSegment(peerServerName);
            meta_->segmentIDs[pollingRank] = segment_id;
            peerState.segmentId = segment_id;

            if (buffer_data.size() < sizeof(SegmentInfo)) {
                peerState.increaseCheckStoreBackoff();
                return false;
            }
            memcpy(&meta_->segmentInfos[pollingRank], buffer_data.data(),
                   sizeof(SegmentInfo));

            if (pollingRank <= rank_) {
                // Send a pre-flight request to establish connections
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
                global_peerConnected_[global_rank] = true;
                peerState.state = PeerConnectionState::CONNECTED;

                {
                    std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
                    totalConnectedPeers_.fetch_add(1,
                                                   std::memory_order_release);
                    if (isAllPeerConnected()) backend_wakeup_cv_.notify_all();
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
                peerState.state = PeerConnectionState::CONNECTED;
                {
                    std::lock_guard<std::mutex> lock(backend_wakeup_mutex_);
                    totalConnectedPeers_.fetch_add(1,
                                                   std::memory_order_release);
                    if (isAllPeerConnected()) backend_wakeup_cv_.notify_all();
                }
                state_changed = true;
            }
            break;
        }

        case PeerConnectionState::CONNECTED: {
            if (meta_->peerConnected[pollingRank] && global_peerConnected_[global_rank]) break;
            // If meta_->peerConnected is false but PeerConnectionState is
            // CONNECTED, the peer might be marked as broken for some reason
            // (e.g. timeout in backend worker thread).
            // In that case, back to WAITING_STORE to reconnect it.

            // we also need to broadcast the info to the process level
            global_peerConnected_[global_rank] = false;
            totalConnectedPeers_.fetch_sub(1);
            peerState.state = PeerConnectionState::WAITING_STORE;
            engine_->closeSegment(peerState.segmentId.value());
            peerState.segmentId = std::nullopt;
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

ConnectionPoller::ConnectionPoller() {
    pollerThread_ = std::thread([this] { pollerLoop(); });
}

void ConnectionPoller::registerContext(
    const std::shared_ptr<ConnectionContext>& ctx) {
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
