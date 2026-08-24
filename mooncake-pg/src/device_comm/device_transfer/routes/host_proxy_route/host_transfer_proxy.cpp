#include "device_comm/device_transfer/routes/host_proxy_route/host_transfer_proxy.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <transfer_engine.h>

#include "control_plane/link_manager.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "gpu_runtime.h"
#include "memory_location.h"

namespace mooncake {

struct HostTransferProxy::Lane {
    // `batch_id` belongs to exactly the transfer named by the current state:
    //
    //   WaitingForCommand
    //        -> PayloadTransferInFlight (when the payload is nonempty)
    //        -> SignalReadInFlight       (when the action is Add)
    //        -> SignalWriteInFlight      (when the action is Add)
    //        -> WaitingForCommand
    //
    // Only finishCommand() returns the command slot to the GPU producer.
    enum class State : uint8_t {
        WaitingForCommand,
        PayloadTransferInFlight,
        SignalReadInFlight,
        SignalWriteInFlight,
    };

    ~Lane() noexcept {
        if (batch_id != INVALID_BATCH_ID) {
            LOG(ERROR) << "Destroying a host-proxy lane with an outstanding "
                          "TE batch";
        }
    }

    HostProxyCommandSlot* host_slot = nullptr;
    uint64_t* signal_staging = nullptr;

    State state = State::WaitingForCommand;
    uint64_t sequence = 0;
    HostProxyCommand command;
    TransferMetadata::SegmentID target_segment_id = 0;
    BatchID batch_id = INVALID_BATCH_ID;
};

struct HostTransferProxy::LaneSet {
    explicit LaneSet(TransferEngine& engine) noexcept : engine(engine) {}
    ~LaneSet() noexcept;

    static PGResult<std::unique_ptr<LaneSet>> create(TransferEngine& engine);

    TransferEngine& engine;
    HostProxyCommandSlot* host_slots = nullptr;
    HostProxyCommandSlot* device_slots = nullptr;
    std::array<uint64_t, kTransferLaneCount> signal_staging{};
    bool signal_staging_registered = false;
    std::array<Lane, kTransferLaneCount> lanes{};
};

HostTransferProxy::LaneSet::~LaneSet() noexcept {
    if (signal_staging_registered) {
        const int result = engine.unregisterLocalMemory(signal_staging.data());
        if (result != 0) {
            LOG(ERROR) << "Failed to unregister host-proxy signal staging, rc="
                       << result;
        }
    }

    if (host_slots) {
        std::destroy_n(host_slots, kTransferLaneCount);
        const auto result = cudaFreeHost(host_slots);
        if (result != cudaSuccess) {
            LOG(ERROR) << "Failed to free host-proxy slots: "
                       << cudaGetErrorString(result);
        }
    }
}

PGResult<std::unique_ptr<HostTransferProxy::LaneSet>>
HostTransferProxy::LaneSet::create(TransferEngine& engine) {
    auto lane_set =
        std::unique_ptr<LaneSet>(new (std::nothrow) LaneSet(engine));
    if (!lane_set) {
        return makePGError(PGErrorCode::SystemError,
                           "failed to allocate fixed host-proxy lanes");
    }

    constexpr size_t slot_count = kTransferLaneCount;
    constexpr size_t slot_size = slot_count * sizeof(HostProxyCommandSlot);
    PG_TRY_CUDA(cudaHostAlloc(reinterpret_cast<void**>(&lane_set->host_slots),
                              slot_size,
                              cudaHostAllocMapped | cudaHostAllocPortable));
    std::uninitialized_value_construct_n(lane_set->host_slots, slot_count);

    PG_TRY_CUDA(cudaHostGetDevicePointer(
        reinterpret_cast<void**>(&lane_set->device_slots), lane_set->host_slots,
        0));

    PG_TRY_TE(engine.registerLocalMemory(lane_set->signal_staging.data(),
                                         sizeof(lane_set->signal_staging),
                                         kWildcardLocation,
                                         /*remote_accessible=*/false));
    lane_set->signal_staging_registered = true;

    for (uint32_t index = 0; index < kTransferLaneCount; ++index) {
        auto& lane = lane_set->lanes[index];
        lane.host_slot = lane_set->host_slots + index;
        lane.signal_staging = lane_set->signal_staging.data() + index;
    }
    return lane_set;
}

uint64_t HostTransferProxy::loadSubmitted(const Lane& lane) {
    return std::atomic_ref(lane.host_slot->submitted_sequence)
        .load(std::memory_order_acquire);
}

uint64_t HostTransferProxy::loadCompleted(const Lane& lane) {
    return std::atomic_ref(lane.host_slot->completed_sequence)
        .load(std::memory_order_acquire);
}

bool HostTransferProxy::laneIdle(const Lane& lane) {
    return loadSubmitted(lane) == loadCompleted(lane);
}

bool HostTransferProxy::laneSetIdle(const LaneSet& lane_set) {
    for (const auto& lane : lane_set.lanes) {
        if (!laneIdle(lane)) return false;
    }
    return true;
}

bool HostTransferProxy::lanesIdle() const {
    return !lane_set_ || laneSetIdle(*lane_set_);
}

void HostTransferProxy::finishCommand(Lane& lane,
                                      HostProxyCommandResult result) {
    const uint64_t sequence = lane.sequence;
    lane.state = Lane::State::WaitingForCommand;
    lane.sequence = 0;
    lane.command = {};
    lane.target_segment_id = 0;

    // Publish completion only after the worker-private lane state is idle, so
    // lifecycle checks need only compare the shared slot sequences.
    lane.host_slot->result = result;
    std::atomic_ref(lane.host_slot->completed_sequence)
        .store(sequence, std::memory_order_release);
    state_changed_.notify_all();
}

void HostTransferProxy::releaseBatch(Lane& lane) {
    if (lane.batch_id == INVALID_BATCH_ID) return;
    const auto batch_id = std::exchange(lane.batch_id, INVALID_BATCH_ID);
    const auto status = engine_.freeBatchID(batch_id);
    if (!status.ok()) {
        LOG(WARNING) << "Host-proxy TE BatchID leaked after "
                        "freeBatchID failed: "
                     << status.message();
    }
}

bool HostTransferProxy::submitBatch(Lane& lane,
                                    const TransferRequest& request) {
    lane.batch_id = engine_.allocateBatchID(1);
    if (lane.batch_id == INVALID_BATCH_ID) return false;

    const auto status = engine_.submitTransfer(lane.batch_id, {request});
    if (status.ok()) return true;

    LOG(WARNING) << "Host-proxy TE submission failed: " << status.message();
    releaseBatch(lane);
    return false;
}

HostTransferProxy::BatchPollResult HostTransferProxy::pollBatch(Lane& lane) {
    TransferStatus transfer_status{};
    const auto status =
        engine_.getTransferStatus(lane.batch_id, 0, transfer_status);
    if (!status.ok()) {
        LOG(WARNING) << "Host-proxy TE status query failed: "
                     << status.message();
        releaseBatch(lane);
        return BatchPollResult::Failed;
    }
    switch (transfer_status.s) {
        case TransferStatusEnum::COMPLETED:
            releaseBatch(lane);
            return BatchPollResult::Succeeded;
        case TransferStatusEnum::WAITING:
        case TransferStatusEnum::PENDING:
            return BatchPollResult::InFlight;
        default:
            releaseBatch(lane);
            return BatchPollResult::Failed;
    }
}

bool HostTransferProxy::tryStartCommand(Lane& lane) {
    const uint64_t completed = loadCompleted(lane);
    const uint64_t submitted = loadSubmitted(lane);
    if (submitted == completed) return false;

    lane.sequence = submitted;
    // loadSubmitted() acquired the GPU's publication. Snapshot the shared
    // command before starting asynchronous TE work.
    lane.command = lane.host_slot->command;

    // A producer may only publish the next sequence after observing the
    // previous completion. Anything else means the single-slot protocol
    // was violated; fail the observed command instead of guessing.
    if (completed == UINT64_MAX || submitted != completed + 1) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }

    const auto rank = lane.command.target_rank;
    if (rank < 0 || static_cast<uint32_t>(rank) >= max_world_size_) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }
    const auto segment = link_manager_.resolvePeer(rank);
    if (!segment) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }
    lane.target_segment_id = *segment;

    if (lane.command.size == 0) {
        startSignalAction(lane);
        return true;
    }
    if (lane.command.local_addr == 0 || lane.command.remote_region_addr == 0) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }

    startPayloadTransfer(lane);
    return true;
}

void HostTransferProxy::startPayloadTransfer(Lane& lane) {
    lane.state = Lane::State::PayloadTransferInFlight;
    if (!submitBatch(
            lane,
            TransferRequest{
                .opcode = TransferRequest::WRITE,
                .source = reinterpret_cast<void*>(lane.command.local_addr),
                .target_id = lane.target_segment_id,
                .target_offset = lane.command.remote_region_addr +
                                 lane.command.remote_offset,
                .length = static_cast<size_t>(lane.command.size),
            })) {
        finishCommand(lane, HostProxyCommandResult::Failed);
    }
}

void HostTransferProxy::startSignalAction(Lane& lane) {
    switch (lane.command.signal.kind) {
        case SignalAction::Kind::None:
            finishCommand(lane, HostProxyCommandResult::Succeeded);
            return;
        case SignalAction::Kind::Add:
            startSignalRead(lane);
            return;
    }
    finishCommand(lane, HostProxyCommandResult::Failed);
}

void HostTransferProxy::startSignalRead(Lane& lane) {
    // TE exposes READ/WRITE but no remote atomic add. The single-publisher
    // contract lets the fallback route implement add as a remote read followed
    // by a write through this lane's registered staging word.
    lane.state = Lane::State::SignalReadInFlight;
    if (!submitBatch(lane,
                     TransferRequest{
                         .opcode = TransferRequest::READ,
                         .source = lane.signal_staging,
                         .target_id = lane.target_segment_id,
                         .target_offset = lane.command.remote_region_addr +
                                          lane.command.signal.add.remote_offset,
                         .length = sizeof(uint64_t),
                     })) {
        finishCommand(lane, HostProxyCommandResult::Failed);
    }
}

void HostTransferProxy::startSignalWrite(Lane& lane) {
    *lane.signal_staging += lane.command.signal.add.delta;
    lane.state = Lane::State::SignalWriteInFlight;
    if (!submitBatch(lane,
                     TransferRequest{
                         .opcode = TransferRequest::WRITE,
                         .source = lane.signal_staging,
                         .target_id = lane.target_segment_id,
                         .target_offset = lane.command.remote_region_addr +
                                          lane.command.signal.add.remote_offset,
                         .length = sizeof(uint64_t),
                     })) {
        finishCommand(lane, HostProxyCommandResult::Failed);
    }
}

bool HostTransferProxy::stepPayloadTransfer(Lane& lane) {
    const auto result = pollBatch(lane);
    if (result == BatchPollResult::InFlight) return false;
    if (result == BatchPollResult::Failed) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }
    startSignalAction(lane);
    return true;
}

bool HostTransferProxy::stepSignalRead(Lane& lane) {
    const auto result = pollBatch(lane);
    if (result == BatchPollResult::InFlight) return false;
    if (result == BatchPollResult::Failed) {
        finishCommand(lane, HostProxyCommandResult::Failed);
        return true;
    }
    startSignalWrite(lane);
    return true;
}

bool HostTransferProxy::stepSignalWrite(Lane& lane) {
    const auto result = pollBatch(lane);
    if (result == BatchPollResult::InFlight) return false;
    finishCommand(lane, result == BatchPollResult::Succeeded
                            ? HostProxyCommandResult::Succeeded
                            : HostProxyCommandResult::Failed);
    return true;
}

bool HostTransferProxy::step(Lane& lane) {
    switch (lane.state) {
        case Lane::State::WaitingForCommand:
            return tryStartCommand(lane);
        case Lane::State::PayloadTransferInFlight:
            return stepPayloadTransfer(lane);
        case Lane::State::SignalReadInFlight:
            return stepSignalRead(lane);
        case Lane::State::SignalWriteInFlight:
            return stepSignalWrite(lane);
    }
    return false;
}

void HostTransferProxy::run() noexcept {
    try {
        while (true) {
            LaneSet* lane_set = nullptr;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (shutdown_requested_) break;
                lane_set = lane_set_.get();
            }

            bool progressed = false;
            if (lane_set) {
                for (auto& lane : lane_set->lanes) {
                    progressed |= step(lane);
                }
            }

            if (!progressed) {
                std::unique_lock<std::mutex> lock(mutex_);
                state_changed_.wait_for(lock, kWorkerPollInterval,
                                        [this] { return shutdown_requested_; });
            }
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "HostTransferProxy worker stopped after an "
                      "exception: "
                   << error.what();
        std::lock_guard<std::mutex> lock(mutex_);
        terminated_with_error_ = true;
        state_changed_.notify_all();
    } catch (...) {
        LOG(ERROR) << "HostTransferProxy worker stopped after an unknown "
                      "exception";
        std::lock_guard<std::mutex> lock(mutex_);
        terminated_with_error_ = true;
        state_changed_.notify_all();
    }
}

void HostTransferProxy::stopWorker() noexcept {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_requested_ = true;
    }
    state_changed_.notify_all();
    if (worker_.joinable()) worker_.join();
}

HostTransferProxy::HostTransferProxy(TransferEngine& engine,
                                     LinkManager& link_manager,
                                     uint32_t max_world_size)
    : engine_(engine),
      link_manager_(link_manager),
      max_world_size_(max_world_size) {}

HostTransferProxy::~HostTransferProxy() noexcept {
    const auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "HostTransferProxy shutdown failed during destruction: "
                   << result.error().message;
        stopWorker();

        // An in-flight TE batch or a parked GPU kernel may still reference
        // LaneSet storage. Abandon the complete owner rather than freeing only
        // part of it during failed teardown.
        std::lock_guard<std::mutex> lock(mutex_);
        auto leak = lane_set_.release();
        (void)leak;
    }
}

PGResult<void> HostTransferProxy::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(!shutdown_requested_, "HostTransferProxy is shut down");
    if (started_) return {};
    try {
        worker_ = std::thread([this] { run(); });
    } catch (const std::exception& error) {
        return makePGError(
            PGErrorCode::SystemError,
            std::string("failed to start HostTransferProxy: ") + error.what());
    }
    started_ = true;
    return {};
}

PGResult<HostProxyCommandSlot*> HostTransferProxy::initializeDevice(
    int device_index) {
    PG_VALIDATE_ARG(device_index >= 0, "invalid host-proxy CUDA device");

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(
        started_ && !shutdown_requested_ && !terminated_with_error_,
        "HostTransferProxy is not running");
    PG_VALIDATE_STATE(!lane_set_,
                      "HostTransferProxy device is already initialized");

    PG_TRY(auto lane_set, LaneSet::create(engine_));

    auto* const device_slots = lane_set->device_slots;
    lane_set_ = std::move(lane_set);
    state_changed_.notify_all();
    return device_slots;
}

PGResult<void> HostTransferProxy::waitUntilIdle() {
    std::unique_lock<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(lane_set_, "host-proxy CUDA device is not initialized");
    state_changed_.wait(
        lock, [this] { return terminated_with_error_ || lanesIdle(); });
    PG_VALIDATE_STATE(!terminated_with_error_,
                      "HostTransferProxy worker has failed");
    return {};
}

PGResult<void> HostTransferProxy::waitUntilIdle(
    std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(lane_set_, "host-proxy CUDA device is not initialized");
    const bool ready = state_changed_.wait_for(lock, timeout, [this] {
        return terminated_with_error_ || lanesIdle();
    });
    if (!ready) {
        return makePGError(PGErrorCode::Timeout,
                           "host-proxy device did not become idle in time");
    }
    PG_VALIDATE_STATE(!terminated_with_error_,
                      "HostTransferProxy worker has failed");
    return {};
}

PGResult<void> HostTransferProxy::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_requested_) return {};
        PG_VALIDATE_STATE(lanesIdle(),
                          "HostTransferProxy still has in-flight commands");
        shutdown_requested_ = true;
    }
    stopWorker();

    std::unique_ptr<LaneSet> released_lanes;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        released_lanes = std::move(lane_set_);
    }
    // LaneSet releases CUDA and TE resources without holding the proxy mutex.
    released_lanes.reset();
    return {};
}

}  // namespace mooncake
