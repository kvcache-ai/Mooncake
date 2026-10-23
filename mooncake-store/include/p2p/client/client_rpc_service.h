#pragma once

#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <async_simple/coro/Lazy.h>
#include <ylt/util/tl/expected.hpp>
#include "p2p/client/client_rpc_types.h"
#include "p2p/client/data_manager.h"
#include "inflight_tracker.h"
#include "p2p/client/p2p_client_metric.h"
#include "p2p/common/p2p_types.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake {

/**
 * @class ClientRpcService
 * @brief RPC service for handling remote data read/write requests from peer
 * clients
 *
 * This service receives RPC requests from other clients (e.g., Client A) to
 * read/write data stored locally (on Client B). It uses DataManager to access
 * TieredBackend and TransferEngine to perform zero-copy RDMA transfers.
 */
class ClientRpcService {
   public:
    /**
     * @brief Constructor
     * @param data_manager Reference to DataManager instance (must outlive this
     * object)
     * @param metrics Optional pointer to P2PClientMetric for recording peer
     * request metrics
     */
    explicit ClientRpcService(
        DataManager& data_manager,
        std::shared_ptr<P2PClientMetric> metrics = nullptr);

    // After local callers have drained, wait for peer handlers and leases,
    // then close and drain the completion handlers. The listener stays alive
    // throughout this call so peers can return their operation tokens.
    void Stop();

    // Enable peer requests after local resources and runtime config are ready.
    void SetReady() { ready_.store(true, std::memory_order_release); }

    bool IsReady() const {
        return ready_.load(std::memory_order_acquire) &&
               normal_op_tracker_.is_running();
    }

    /**
     * @brief Read remote data: Client A requests Client B to read data and
     * transfer to A
     * @param request RemoteReadRequest containing key and destination buffers
     * @return ErrorCode indicating success or failure
     *
     * Flow:
     * 1. DataManager.ReadRemoteDataAsync (TE wait may yield via te_poll)
     * 2. TieredBackend.Get(key) → handle
     * 3. TransferEngine.submitTransfer(WRITE) to transfer data from B to A
     */
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> ReadRemoteData(
        const RemoteReadRequest& request);

    /**
     * @brief Write remote data: Client A requests Client B to write data from A
     * @param request RemoteWriteRequest containing key, source buffers, and
     * target_tier_id
     * @return UUID containing the route descriptor of the
     *         written replica, or ErrorCode
     */
    async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>> WriteRemoteData(
        const RemoteWriteRequest& request);

    tl::expected<PreWriteResponse, ErrorCode> PreWrite(
        const PreWriteRequest& request);

    tl::expected<void, ErrorCode> WriteCommit(
        const WriteCommitRequest& request);

    tl::expected<void, ErrorCode> WriteRevoke(
        const WriteRevokeRequest& request);

    tl::expected<PinKeyResponse, ErrorCode> PinKey(
        const PinKeyRequest& request);

    tl::expected<void, ErrorCode> UnPinKey(const UnPinKeyRequest& request);

   private:
    void RecordPeerInflight(bool entering);

   private:
    DataManager& data_manager_;  // Reference: owned by Client, same lifetime
    std::shared_ptr<P2PClientMetric>
        metrics_;  // Optional: shared from P2PClientService

    std::atomic<bool> ready_{false};
    // Normal operation RPCs: ReadRemoteData, WriteRemoteData, PreWrite and
    // PinKey.
    InflightTracker normal_op_tracker_;
    // RPCs that complete or release existing leases: WriteCommit, WriteRevoke
    // and UnPinKey. Keep accepting them while new operations are draining,
    // then wait for their handlers after the lease records have drained.
    InflightTracker finish_op_tracker_;
};

/**
 * @brief Register ClientRpcService methods with coro_rpc_server
 * @param server coro_rpc_server instance
 * @param service ClientRpcService instance
 */
void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service);

}  // namespace mooncake
