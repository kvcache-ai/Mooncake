#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_arena.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "device_comm/device_collective/device_collective_recovery.h"
#include "error_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "gpu_runtime.h"

namespace mooncake {

class StrongStream;

class DeviceCollectiveRuntime {
   public:
    using RecoveryHandler = std::function<PGResult<void>(InGroupRank)>;

    static PGResult<std::unique_ptr<DeviceCollectiveRuntime>> create(
        DeviceTransferService& transfer_service, DeviceArena& arena,
        const DeviceArenaSlice& workspace, StrongStream& strong_stream,
        int device_index, InGroupRank self_rank, uint32_t max_group_size,
        size_t collective_timeout_us);

    ~DeviceCollectiveRuntime() noexcept;

    DeviceCollectiveRuntime(const DeviceCollectiveRuntime&) = delete;
    DeviceCollectiveRuntime& operator=(const DeviceCollectiveRuntime&) = delete;

    [[nodiscard]] const DeviceCollectiveEndpoint& localEndpoint() const;

    PGResult<void> useLocalOnly();
    PGResult<void> applyGroupView(const GroupView& view);

    PGResult<void> enableRecovery(DeviceCollectiveRecoveryWorker& worker,
                                  RecoveryHandler handler);

    PGResult<void> enqueueAllReduce(const void* send_buffer, void* recv_buffer,
                                    size_t count, DataType datatype,
                                    ReduceOp op,
                                    cudaStream_t user_stream_handle,
                                    int32_t* failed_ranks_hint);

    // Graceful shutdown waits until every submitted collective has completed.
    PGResult<void> shutdown();

   private:
    friend class MooncakeCommunicator;

    static constexpr size_t kChannelScaleUnitBytes = 4ull << 20;  // 4 MiB

    struct ControlSliceLayout {
        static constexpr uint64_t kAlignment = 256;

        uint64_t size = 0;
        uint64_t all_reduce_plan_offset = 0;
        uint64_t next_step_sequences_offset = 0;
        uint64_t next_recv_ready_sequences_offset = 0;
        uint64_t invocation_offset = 0;
        uint64_t recv_ready_slots_offset = 0;
        uint64_t signal_slots_offset = 0;
        uint64_t consumed_ack_slots_offset = 0;
        uint32_t max_group_size = 0;

        static ControlSliceLayout make(uint32_t max_group_size);

        [[nodiscard]] DeviceCollectiveControlView map(
            const DeviceArenaSlice& control_slice) const;
    };

    struct HostControl;

    DeviceCollectiveRuntime(DeviceTransferService& transfer_service,
                            int device_index, InGroupRank self_rank,
                            uint64_t timeout_ticks, ControlSliceLayout layout,
                            DeviceArenaSlice control_slice,
                            DeviceCollectiveKernelResources kernel_resources,
                            StrongStream& strong_stream,
                            DeviceCollectiveEndpoint endpoint,
                            GpuStream control_stream, GpuEvent handoff_event);

    PGResult<void> initializeHostControl();
    PGResult<void> publishAllReducePlan(DeviceAllReducePlan plan);
    PGResult<void> invalidateAllReducePlan();
    PGResult<void> attachGraphUse(const GpuCaptureInfo& capture);
    PGResult<void> recoverFailure();
    static uint32_t chooseChannelCount(size_t size);
    void releaseHostControl() noexcept;

    DeviceTransferService& transfer_service_;
    int device_index_ = -1;
    InGroupRank self_rank_ = kInvalidInGroupRank;
    uint64_t timeout_ticks_ = 0;
    ControlSliceLayout layout_;
    DeviceArenaSlice control_slice_;
    StrongStream& strong_stream_;
    DeviceCollectiveEndpoint endpoint_;
    HostControl* host_control_ = nullptr;
    DeviceCollectiveKernelResources kernel_resources_;
    bool host_all_reduce_plan_ready_ = false;
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
