#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_arena.h"
#include "device_comm/device_collective/device_collective_recovery.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_transfer/transfer_service.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class DeviceCollectiveWorkspace;
class RingAllReduceProtocol;
class StrongStream;

// Protocol-independent lifecycle facade. It owns communicator invocation and
// recovery state, stream ordering and graph references; the selected protocol
// owns topology, publication, resource interpretation and kernel launch policy.
class DeviceCollectiveRuntime {
   public:
    using RecoveryHandler = std::function<PGResult<void>(InGroupRank)>;

    static PGResult<std::unique_ptr<DeviceCollectiveRuntime>> create(
        DeviceTransferService& transfer_service, DeviceArena& arena,
        DeviceCollectiveWorkspace& workspace, StrongStream& strong_stream,
        int device_index, InGroupRank self_rank, uint32_t max_group_size,
        size_t collective_timeout_us);

    ~DeviceCollectiveRuntime() noexcept;

    DeviceCollectiveRuntime(const DeviceCollectiveRuntime&) = delete;
    DeviceCollectiveRuntime& operator=(const DeviceCollectiveRuntime&) = delete;

    [[nodiscard]] DeviceCollectiveProtocolEndpoints localEndpoints() const;

    PGResult<void> useLocalOnly();
    PGResult<void> applyGroupView(const GroupView& view);

    PGResult<void> enableRecovery(DeviceCollectiveRecoveryWorker& worker,
                                  RecoveryHandler handler);

    PGResult<void> enqueueAllReduce(const void* send_buffer, void* recv_buffer,
                                    size_t count, DataType datatype,
                                    ReduceOp op,
                                    cudaStream_t user_stream_handle,
                                    int32_t* failed_ranks_hint);

    PGResult<void> shutdown();

   private:
    friend class MooncakeCommunicator;

    DeviceCollectiveRuntime(DeviceTransferService& transfer_service,
                            int device_index, StrongStream& strong_stream,
                            GpuStream control_stream, GpuEvent handoff_event);

    PGResult<void> attachGraphUse(const GpuCaptureInfo& capture);
    PGResult<void> recoverFailure();
    void releaseState() noexcept;

    DeviceTransferService& transfer_service_;
    int device_index_ = -1;
    DeviceCollectiveInvocationState* invocation_state_ = nullptr;
    std::unique_ptr<RingAllReduceProtocol> all_reduce_;
    StrongStream& strong_stream_;
    DeviceCollectiveRecoveryMailbox* recovery_mailbox_ = nullptr;
    RecoveryHandler recovery_handler_;
    DeviceCollectiveRecoveryWorker* recovery_worker_ = nullptr;

    GpuStream control_stream_;
    GpuEvent handoff_event_;
    mutable std::mutex mutex_;
    std::atomic<size_t> live_graph_uses_{0};
    bool shutdown_requested_ = false;
    bool shutdown_complete_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
