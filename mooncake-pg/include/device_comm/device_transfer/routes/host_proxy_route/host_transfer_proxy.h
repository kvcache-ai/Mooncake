#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_TRANSFER_PROXY_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_TRANSFER_PROXY_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include <transfer_engine.h>

#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_types.cuh"
#include "error_types.h"

namespace mooncake {

class LinkManager;

// Transfer-service-owned consumer for commands that cannot use a direct
// device path.
class HostTransferProxy {
   public:
    HostTransferProxy(TransferEngine& engine, LinkManager& link_manager,
                      uint32_t max_world_size);
    ~HostTransferProxy() noexcept;

    HostTransferProxy(const HostTransferProxy&) = delete;
    HostTransferProxy& operator=(const HostTransferProxy&) = delete;

    PGResult<void> start();

    // Create the fixed command-slot set used by the transfer-service device.
    PGResult<HostProxyCommandSlot*> initializeDevice(int device_index);

    PGResult<void> waitUntilIdle();
    PGResult<void> waitUntilIdle(std::chrono::milliseconds timeout);
    PGResult<void> shutdown();

   private:
    static constexpr auto kWorkerPollInterval = std::chrono::microseconds(50);

    struct Lane;
    struct LaneSet;

    enum class BatchPollResult : uint8_t {
        InFlight,
        Succeeded,
        Failed,
    };

    // These helpers inspect state protected by mutex_.
    static uint64_t loadSubmitted(const Lane& lane);
    static uint64_t loadCompleted(const Lane& lane);
    static bool laneIdle(const Lane& lane);
    static bool laneSetIdle(const LaneSet& lane_set);
    bool lanesIdle() const;

    void finishCommand(Lane& lane, HostProxyCommandResult result);
    void releaseBatch(Lane& lane);
    bool submitBatch(Lane& lane, const TransferRequest& request);
    BatchPollResult pollBatch(Lane& lane);
    bool tryStartCommand(Lane& lane);
    void startPayloadTransfer(Lane& lane);
    void startSignalRead(Lane& lane);
    void startSignalWrite(Lane& lane);
    bool stepPayloadTransfer(Lane& lane);
    bool stepSignalRead(Lane& lane);
    bool stepSignalWrite(Lane& lane);
    bool step(Lane& lane);
    void run() noexcept;
    void stopWorker() noexcept;

    TransferEngine& engine_;
    LinkManager& link_manager_;
    uint32_t max_world_size_ = 0;
    mutable std::mutex mutex_;
    std::condition_variable state_changed_;
    std::unique_ptr<LaneSet> lane_set_;
    std::thread worker_;
    bool started_ = false;
    bool shutdown_requested_ = false;
    bool terminated_with_error_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_TRANSFER_PROXY_H
