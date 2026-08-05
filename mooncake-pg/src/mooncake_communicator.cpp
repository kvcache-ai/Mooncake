#include <mooncake_communicator.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <limits>
#include <utility>

#include <glog/logging.h>

#include "common.h"
#include "error_types.h"
#include "gpu_runtime.h"
#include "memory_location.h"

namespace mooncake {
namespace {

// A non-zero operation size is required to ensure that the worker creates a
// task for the barrier.
constexpr size_t kBarrierDummySize = 1;

void copyDeviceToDevice(void* dst, const void* src, size_t bytes,
                        cudaStream_t stream) {
    PG_ASSERT_CUDA(
        cudaMemcpyAsync(dst, src, bytes, cudaMemcpyDeviceToDevice, stream));
}

PGResult<void> checkBuffer(const void* buffer, size_t bytes, const char* name) {
    PG_VALIDATE_ARG(buffer || bytes == 0, std::string(name) + " is null");
    return {};
}

PGResult<void> checkRoot(int root, int max_group_size, const char* operation) {
    PG_VALIDATE_ARG(root >= 0 && root < max_group_size,
                    std::string(operation) + " root is out of range");
    return {};
}

PGResult<void> checkP2PPeer(const TransferGroupMeta& meta, int peer,
                            int max_group_size, const char* operation) {
    PG_VALIDATE_ARG(peer >= 0 && peer < max_group_size,
                    std::string(operation) + " peer is out of range");
    // P2P may target inactive members, but reserved extension slots are not
    // valid targets until they have an assigned global-rank mapping.
    PG_VALIDATE_ARG(
        meta.rank_order[peer] != kInvalidGlobalRank,
        std::string(operation) + " peer is not assigned in this group");
    return {};
}

PGResult<size_t> getByteCount(size_t count, DataType datatype) {
    switch (datatype) {
        case DataType::Int8:
        case DataType::Uint8:
        case DataType::Int16:
        case DataType::Uint16:
        case DataType::Int32:
        case DataType::Uint32:
        case DataType::Int64:
        case DataType::Uint64:
        case DataType::Float16:
        case DataType::Float32:
        case DataType::Float64:
        case DataType::Bfloat16:
        case DataType::Bool:
        case DataType::Float8e4m3fn:
        case DataType::Float8e5m2:
        case DataType::Float8e4m3fnuz:
        case DataType::Float8e5m2fnuz:
        case DataType::Float8e8m0fnu:
            break;
        default:
            return makePGError(PGErrorCode::InvalidArgument,
                               "unsupported Mooncake PG datatype");
    }

    const size_t element_size = elementSize(datatype);
    PG_VALIDATE_ARG(count <= std::numeric_limits<size_t>::max() / element_size,
                    "element count overflows size_t");
    return count * element_size;
}

PGResult<void> checkReduction(DataType datatype, ReduceOp op, bool is_cpu) {
    switch (op) {
        case ReduceOp::Sum:
        case ReduceOp::Product:
        case ReduceOp::Min:
        case ReduceOp::Max:
            break;
        case ReduceOp::Avg:
            return makePGError(PGErrorCode::NotSupported,
                               "average reduction is not supported");
        default:
            return makePGError(PGErrorCode::NotSupported,
                               "reduction operation is not supported");
    }

    switch (datatype) {
        case DataType::Uint8:
        case DataType::Int8:
        case DataType::Int16:
        case DataType::Int32:
        case DataType::Int64:
        case DataType::Float32:
        case DataType::Float64:
        case DataType::Bool:
            break;
        case DataType::Bfloat16:
            if (!is_cpu) break;
            [[fallthrough]];
        default:
            return makePGError(PGErrorCode::NotSupported,
                               "reduction datatype is not supported");
    }

    return {};
}

}  // namespace

MooncakePGContext::~MooncakePGContext() {
    try {
        auto result = shutdown();
        if (!result.has_value()) {
            LOG(ERROR)
                << "Mooncake PG context shutdown failed during destruction: "
                << result.error().message;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "Mooncake PG context shutdown failed during destruction: "
                   << error.what();
    } catch (...) {
        LOG(ERROR) << "Mooncake PG context shutdown failed during destruction";
    }
}

PGResult<void> MooncakePGContext::checkRunning() const {
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "Mooncake PG context is shutting down");
    return {};
}

PGResult<void> MooncakePGContext::initialize(int rank, int world_size) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_ARG(world_size > 0 && world_size <= kMaxNumRanks,
                    "max_world_size is outside the supported range");
    PG_VALIDATE_ARG(rank >= 0 && rank < world_size,
                    "global rank is outside the process world");
    if (initialized_) {
        PG_VALIDATE_STATE(
            global_rank == rank && max_world_size == world_size,
            "Mooncake process context was initialized with a different rank "
            "or world size");
        return {};
    }

    // Ordering constraint: AgentHost::start() sends registerAgent immediately,
    // which includes LinkManager's localServerName() and getWarmupRecvAddr().
    // These must be non-empty, so the engine and LinkManager must be
    // initialized before connectCoordinator starts the AgentHost.
    if (!engine_initialized) {
#ifdef USE_MACA
        PG_VALIDATE_ARG(std::getenv("MC_MACA_HOST_TRANSPORT") != nullptr,
                        "MACA PG requires MC_MACA_HOST_TRANSPORT=1");
#endif
        PG_TRY_TE(engine->init(P2PHANDSHAKE, host_ip));
        engine_initialized = true;
    }
    if (!link_manager.isInitialized()) {
        PG_TRY(link_manager.init(rank, world_size, engine));
    }

    global_rank = rank;
    max_world_size = world_size;
    initialized_ = true;
    return {};
}

PGResult<std::string> MooncakePGContext::launchCoordinator() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_STATE(
        initialized_,
        "Mooncake PG context must be initialized before launching the "
        "coordinator");
    PG_VALIDATE_STATE(global_rank == 0,
                      "only global rank 0 may start the coordinator");
    if (!coordinator_host) {
        auto candidate = std::make_unique<CoordinatorHost>(
            host_ip, max_world_size, fault_reconciliation_window_us);
        PG_TRY(candidate->start());
        coordinator_host = std::move(candidate);
    }
    const auto& address = coordinator_host->getListenAddr();
    if (address.empty()) {
        return makePGError(PGErrorCode::SystemError,
                           "coordinator returned an empty address");
    }
    return address;
}

PGResult<void> MooncakePGContext::connectCoordinator(
    const std::string& coordinator_address) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_STATE(
        initialized_,
        "Mooncake PG context must be initialized before connecting to the "
        "coordinator");
    PG_VALIDATE_ARG(!coordinator_address.empty(),
                    "coordinator address must not be empty");
    if (!agent_host) {
        auto candidate = std::make_unique<AgentHost>(
            coordinator_address, host_ip, global_rank, max_world_size,
            link_manager, fault_reconciliation_window_us);
        PG_TRY(candidate->start());
        agent_host = std::move(candidate);
    }
    return {};
}

PGResult<void> MooncakePGContext::setHostIp(std::string value) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_ARG(!value.empty(), "host IP must not be empty");
    PG_VALIDATE_STATE(!initialized_ || host_ip == value,
                      "host IP cannot be changed after context initialization");
    if (!initialized_) host_ip = std::move(value);
    return {};
}

PGResult<void> MooncakePGContext::setExternalEngine(
    TransferEngine* transfer_engine) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    auto* requested_engine =
        transfer_engine ? transfer_engine : owned_engine.get();
    PG_VALIDATE_STATE(
        !initialized_ || engine == requested_engine,
        "transfer engine cannot be changed after context initialization");
    if (!initialized_) {
        engine = requested_engine;
        engine_initialized = transfer_engine != nullptr;
        if (transfer_engine) {
            const auto endpoint = engine->getLocalIpAndPort();
            const auto derived_host = getHostNameWithoutPort(endpoint);
            PG_VALIDATE_STATE(
                !derived_host.empty(),
                "set_transfer_engine requires an initialized TransferEngine "
                "with a local endpoint");
            host_ip = derived_host;
        }
    }
    return {};
}

PGResult<void> MooncakePGContext::setDeviceFilter(
    std::vector<std::string> filters) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    std::sort(filters.begin(), filters.end());
    filters.erase(std::unique(filters.begin(), filters.end()), filters.end());
    PG_VALIDATE_STATE(
        !initialized_ || device_filters_ == filters,
        "device filters cannot be changed after context initialization");
    if (!initialized_) {
        device_filters_ = filters;
        engine->setWhitelistFilters(std::move(filters));
    }
    return {};
}

PGResult<void> MooncakePGContext::setCollectiveTimeout(size_t timeout_us) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    collective_timeout_us = timeout_us;
    return {};
}

PGResult<void> MooncakePGContext::setP2PTimeout(int64_t timeout_us) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_ARG(timeout_us >= 0, "P2P timeout must not be negative");
    p2p_timeout_us = timeout_us;
    return {};
}

PGResult<void> MooncakePGContext::setFaultReconciliationWindow(
    int64_t timeout_us) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_ARG(timeout_us >= 0,
                    "fault reconciliation window must not be negative");
    fault_reconciliation_window_us = timeout_us;
    if (agent_host) {
        agent_host->setFaultReconciliationWindow(timeout_us);
    }
    if (coordinator_host) {
        PG_TRY(coordinator_host->setFaultReconciliationWindow(timeout_us));
    }
    return {};
}

PGResult<void> MooncakePGContext::incrementCommUseCount() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    PG_TRY(checkRunning());
    PG_VALIDATE_STATE(
        initialized_,
        "Mooncake PG context must be initialized before creating a "
        "communicator");
    PG_VALIDATE_STATE(
        agent_host,
        "Mooncake PG context must connect to a coordinator before creating "
        "a communicator");
    ++comm_use_count_;
    return {};
}

void MooncakePGContext::decrementCommUseCount() noexcept {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (comm_use_count_ == 0) {
        LOG(ERROR) << "Mooncake PG communicator use count underflow";
        return;
    }
    --comm_use_count_;
}

PGResult<void> MooncakePGContext::shutdown() {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (shutdown_requested_) return {};
        if (comm_use_count_ != 0) {
            return makePGError(
                PGErrorCode::ResourceBusy,
                "Mooncake PG context still has active communicators");
        }
        shutdown_requested_ = true;
    }

    if (agent_host) agent_host->shutdown();
    link_manager.shutdown();
    agent_host.reset();
    if (coordinator_host) coordinator_host->shutdown();
    coordinator_host.reset();
    engine = nullptr;
    return {};
}

/**
 * @brief Initialize Mooncake communicator state
 */
PGResult<std::unique_ptr<MooncakeCommunicator>> MooncakeCommunicator::create(
    MooncakePGContext& context, MooncakeCommunicatorConfig config) {
    PG_VALIDATE_STATE(context.engine,
                      "Mooncake PG context has no transfer engine");
    PG_VALIDATE_STATE(
        context.agent_host,
        "Mooncake PG context must connect to a coordinator before creating "
        "a communicator");

    auto communicator = std::unique_ptr<MooncakeCommunicator>(
        new MooncakeCommunicator(context, config));
    PG_TRY(communicator->initialize(std::move(config)));
    return communicator;
}

MooncakeCommunicator::MooncakeCommunicator(
    MooncakePGContext& context, const MooncakeCommunicatorConfig& config)
    : context_(context),
      agent_(*context_.agent_host),
      rank_(config.rank),
      initial_size_(config.size),
      max_group_size_(config.max_group_size > 0 ? config.max_group_size
                                                : config.size),
      device_index_(config.device_index),
      is_cpu_(config.is_cpu),
      active_ranks_mirror_(config.active_ranks_mirror),
      active_ranks_mirror_is_device_(config.active_ranks_mirror_is_device),
      active_ranks_mirror_device_index_(
          config.active_ranks_mirror_device_index) {}

PGResult<void> MooncakeCommunicator::initialize(
    MooncakeCommunicatorConfig config) {
    PG_VALIDATE_ARG(initial_size_ > 0 && initial_size_ <= max_group_size_,
                    "group size exceeds max_group_size");
    PG_VALIDATE_ARG(max_group_size_ > 0 && max_group_size_ <= kMaxNumRanks,
                    "max_group_size is outside the supported range");
    PG_VALIDATE_ARG(rank_ >= 0 && rank_ < initial_size_,
                    "rank is outside the initial group");
    PG_VALIDATE_ARG(!config.group_bootstrap_id.empty(),
                    "group bootstrap id must not be empty");
    PG_VALIDATE_ARG(
        !config.auto_sync_on_failure || config.auto_deactivate_on_failure,
        "auto_sync_on_failure requires auto_deactivate_on_failure");
    PG_VALIDATE_ARG(
        !active_ranks_mirror_ || config.active_ranks_mirror_count >=
                                     static_cast<size_t>(max_group_size_),
        "active-ranks mirror is too small");
    PG_VALIDATE_ARG(!active_ranks_mirror_ || !active_ranks_mirror_is_device_ ||
                        active_ranks_mirror_device_index_ >= 0,
                    "device active-ranks mirror requires a valid device index");

    PG_VALIDATE_ARG(
        config.global_ranks.size() == static_cast<size_t>(initial_size_),
        "global rank count must equal communicator size");
    std::array<bool, kMaxNumRanks> seen_global_ranks{};
    for (const auto global_rank : config.global_ranks) {
        PG_VALIDATE_ARG(
            global_rank >= 0 && global_rank < context_.max_world_size,
            "global rank is outside the process world");
        PG_VALIDATE_ARG(!seen_global_ranks[global_rank],
                        "global ranks contains duplicates");
        seen_global_ranks[global_rank] = true;
    }
    PG_VALIDATE_ARG(config.global_ranks[rank_] == context_.global_rank,
                    "communicator rank does not map to the process global "
                    "rank");
    auto initial_rank_order = std::move(config.global_ranks);

    // Memory location for device-specific buffers. Always kWildcardLocation for
    // a CPU communicator.
    std::unique_ptr<GpuDeviceGuard> device_guard;
    std::string location = kWildcardLocation;
    if (!is_cpu_) {
        if (device_index_ < 0) {
            PG_TRY_CUDA(cudaGetDevice(&device_index_));
        }
        device_guard = std::make_unique<GpuDeviceGuard>(device_index_);
        location = GPU_PREFIX + std::to_string(device_index_);
    }
    if (active_ranks_mirror_ && active_ranks_mirror_is_device_) {
        active_ranks_mirror_stream_ =
            GpuStream::createNonBlocking(active_ranks_mirror_device_index_);
    }

    // Register collective buffers.
    for (size_t index = 0; index < 2; ++index) {
        if (is_cpu_) {
            send_buffer_[index] = std::malloc(kBufferSize);
            recv_buffer_[index] = std::malloc(kBufferSize);
            PG_ASSERT(send_buffer_[index] && recv_buffer_[index],
                      "failed to allocate CPU collective buffers");
        } else {
            PG_TRY_CUDA(cudaMalloc(&send_buffer_[index], kBufferSize));
            PG_TRY_CUDA(cudaMalloc(&recv_buffer_[index], kBufferSize));
        }
        PG_TRY_TE(context_.engine->registerLocalMemory(send_buffer_[index],
                                                       kBufferSize, location));
        PG_TRY_TE(context_.engine->registerLocalMemory(recv_buffer_[index],
                                                       kBufferSize, location));

        // Register CPU synchronization regions.
        cpu_sync_send_region_[index] = new int32_t[kMaxNumRanks]{};
        cpu_sync_recv_region_[index] = new int32_t[kMaxNumRanks]{};
        PG_TRY_TE(context_.engine->registerLocalMemory(
            cpu_sync_send_region_[index], kMaxNumRanks * sizeof(int32_t),
            kWildcardLocation));
        PG_TRY_TE(context_.engine->registerLocalMemory(
            cpu_sync_recv_region_[index], kMaxNumRanks * sizeof(int32_t),
            kWildcardLocation));
    }

    if (is_cpu_) {
        p2p_device_worker_ =
            context_.p2p_device_worker_manager.getCPUWorker(context_.engine);
        worker_ = context_.worker_manager.GetCPUWorker();
    } else {
        p2p_device_worker_ = context_.p2p_device_worker_manager.getCUDAWorker(
            device_index_, context_.engine);
        worker_ = context_.worker_manager.GetCUDAWorker(device_index_);
        preloadReduceKernels();
    }
    worker_->Start();

    p2p_proxy_ = std::make_shared<P2PProxy>(
        context_.engine,
        P2PProxy::Options{.is_cpu = is_cpu_,
                          .rank = rank_,
                          .size = max_group_size_,
                          .cuda_device_index = device_index_,
                          .p2p_timeout_us = &context_.p2p_timeout_us});
    p2p_device_worker_->registerProxy(p2p_proxy_);

    meta_ = std::make_shared<TransferGroupMeta>();
    for (int index = 0; index < kMaxNumRanks; ++index) {
        meta_->segmentIDs[index] = static_cast<TransferMetadata::SegmentID>(-1);
        meta_->rankEpochs[index] = 0;
        meta_->rankStates[index] = RankState::Offline;
        meta_->rank_order[index] = kInvalidGlobalRank;
    }
    meta_->rank = rank_;
    meta_->globalRank = initial_rank_order[rank_];
    for (int index = 0; index < initial_size_; ++index) {
        meta_->rank_order[index] = initial_rank_order[index];
    }
    meta_->maxGroupSize = max_group_size_;  // slot capacity
    meta_->activeSize.store(initial_size_, std::memory_order_relaxed);
    meta_->taskCount = 0;
    meta_->collectiveTimeoutUs = &context_.collective_timeout_us;
    meta_->engine = context_.engine;
    meta_->communicator = this;
    meta_->autoSyncOnFailure = config.auto_sync_on_failure;
    p2p_proxy_->bindMeta(meta_);

    // Active ranks will be filled by applyViewUpdate, so only allocate their
    // storage here.
    meta_->maybeActivatable = new bool[max_group_size_]{};
    if (is_cpu_) {
        meta_->activeRanks = new bool[max_group_size_]{};
        meta_->activeRanksDevice = meta_->activeRanks;
    } else {
        PG_TRY_CUDA(cudaHostAlloc(&meta_->activeRanks,
                                  max_group_size_ * sizeof(bool),
                                  cudaHostAllocMapped));
        PG_TRY_CUDA(cudaHostGetDevicePointer(&meta_->activeRanksDevice,
                                             meta_->activeRanks, 0));
        std::fill_n(meta_->activeRanks, max_group_size_, false);
    }

    // Initial local endpoint info.
    meta_->segmentInfos[rank_] = GroupEndpointInfo{
        .send_buffer = {reinterpret_cast<uint64_t>(send_buffer_[0]),
                        reinterpret_cast<uint64_t>(send_buffer_[1])},
        .recv_buffer = {reinterpret_cast<uint64_t>(recv_buffer_[0]),
                        reinterpret_cast<uint64_t>(recv_buffer_[1])},
        .send_sync = {reinterpret_cast<uint64_t>(cpu_sync_send_region_[0]),
                      reinterpret_cast<uint64_t>(cpu_sync_send_region_[1])},
        .recv_sync = {reinterpret_cast<uint64_t>(cpu_sync_recv_region_[0]),
                      reinterpret_cast<uint64_t>(cpu_sync_recv_region_[1])},
        .p2p_credit_region =
            reinterpret_cast<uint64_t>(p2p_proxy_->credit_region()),
        .p2p_ack_region = reinterpret_cast<uint64_t>(p2p_proxy_->ack_region()),
    };

    // Control Plane Initialization

    // Wait for Agent registration.
    PG_TRY(agent_.waitUntilRegistered(std::chrono::seconds(30)));

    // The PyTorch-provided group id is only a bootstrap id. The Coordinator
    // resolves it together with rank order into a process-lifetime GroupId. CPU
    // and device communicators use independent namespaces.
    auto bootstrap_id = std::string(is_cpu_ ? "cpu:" : "device:") +
                        std::move(config.group_bootstrap_id);

    // Register this group with the Agent, publish the local endpoint, and block
    // until the Coordinator says it is ready. Group registration is
    // synchronous.
    auto group_result = agent_.registerGroup(
        std::move(bootstrap_id), max_group_size_, std::move(initial_rank_order),
        config.group_resolve_policy, config.auto_deactivate_on_failure, this);
    if (!group_result.has_value()) {
        return makePGError(std::move(group_result).error());
    }
    meta_->group_id = std::move(group_result.value());

    if (!isValidGroup()) {
        // Registration rejection is scoped to this communicator. Keep the Agent
        // and every other group untouched, and use the pre-join local-only
        // collective behavior with an effective {self} membership.
        std::fill_n(meta_->activeRanks, max_group_size_, false);
        meta_->activeRanks[rank_] = true;
        meta_->autoSyncOnFailure = false;
        syncActiveRanksMirror();
        refreshSegmentID(rank_);
        LOG(WARNING) << "Mooncake communicator rank=" << meta_->globalRank
                     << " is using local-only execution because group "
                        "registration was rejected";
        return {};
    }

    PG_TRY(agent_.publishLocalEndpoint(buildEndpointMetadata()));
    PG_TRY(
        agent_.waitUntilGroupReady(meta_->group_id, std::chrono::seconds(300)));

    // Initialize all peer segment IDs from the LinkManager. Subsequent updates
    // (endpoint changes, disconnects) are handled by NotifyLinkRefreshed.
    for (int local = 0; local < max_group_size_; ++local) {
        refreshSegmentID(local);
    }
    return {};
}

MooncakeCommunicator::~MooncakeCommunicator() {
    try {
        auto result = shutdown();
        if (!result.has_value()) {
            LOG(ERROR) << "Mooncake communicator shutdown failed: "
                       << result.error().message;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "Mooncake communicator shutdown failed: " << error.what();
    }
}

int MooncakeCommunicator::getSize() const {
    if (!meta_ || meta_->extensionMode.load(std::memory_order_acquire) !=
                      CollectiveExtensionState::Normal) {
        return initial_size_;
    }
    return meta_->activeSize.load(std::memory_order_acquire);
}

PGResult<void> MooncakeCommunicator::checkOpState(OpType op) const {
    PG_VALIDATE_STATE(!is_shutdown_, "communicator is shut down");
    PG_ASSERT(meta_, "initialized communicator has no group metadata");
    const auto mode = meta_->extensionMode.load(std::memory_order_acquire);
    if (isValidGroup()) {
        PG_VALIDATE_STATE(
            meta_->rankStates[meta_->globalRank] != RankState::Offline,
            "rank " + std::to_string(meta_->globalRank) +
                " is offline and cannot perform operations");
    }
    // P2P operations don't require the rank to be active in the group.
    const bool is_p2p = op == OpType::Send || op == OpType::Recv;
    if (!isValidGroup() && is_p2p) {
        return makePGError(PGErrorCode::NotSupported,
                           "P2P is unavailable for an invalid Mooncake group");
    }
    if (!is_p2p) {
        PG_VALIDATE_STATE(mode != CollectiveExtensionState::Quiescing,
                          "rank is quiescing and cannot issue collectives");
        PG_VALIDATE_STATE(mode == CollectiveExtensionState::Isolated ||
                              meta_->activeRanks[rank_],
                          "rank is not active in this group");
    }
    return {};
}

PGResult<void> MooncakeCommunicator::initializeFailedRanksHint(
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) const {
    PG_VALIDATE_ARG(failed_ranks_hint, "failed-ranks hint is null");
    PG_VALIDATE_ARG(
        failed_ranks_hint_count >= static_cast<size_t>(max_group_size_),
        "failed-ranks hint buffer is too small");
    std::fill_n(failed_ranks_hint, max_group_size_, int32_t{0});
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::sendCpu(
    const void* buffer, size_t count, DataType datatype, int peer,
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "sendCpu requires a CPU communicator");
    return enqueueSend(buffer, count, datatype, peer, nullptr,
                       failed_ranks_hint, failed_ranks_hint_count);
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::sendGpu(
    const void* buffer, size_t count, DataType datatype, int peer,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "sendGpu requires a GPU communicator");
    return enqueueSend(buffer, count, datatype, peer, stream, failed_ranks_hint,
                       failed_ranks_hint_count);
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::enqueueSend(
    const void* buffer, size_t count, DataType datatype, int peer,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_TRY(checkOpState(OpType::Send));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_VALIDATE_ARG(buffer || bytes == 0, "send buffer is null");
    PG_TRY(checkP2PPeer(*meta_, peer, max_group_size_, "P2P send"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    auto completion = std::make_shared<std::promise<void>>();
    auto future = completion->get_future().share();
    auto result = std::make_unique<WorkCompletion>(std::move(future));
    p2p_proxy_->enqueueSend(P2PProxy::SendOp{
        .buffer_ = buffer,
        .size_ = bytes,
        .peer_rank_ = peer,
        .cuda_stream_ = stream,
        .completion_ = completion,
        .failed_ranks_hint_ = failed_ranks_hint,
    });
    return result;
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::recvCpu(
    void* buffer, size_t count, DataType datatype, int peer,
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "recvCpu requires a CPU communicator");
    return enqueueRecv(buffer, count, datatype, peer, nullptr,
                       failed_ranks_hint, failed_ranks_hint_count);
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::recvGpu(
    void* buffer, size_t count, DataType datatype, int peer,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "recvGpu requires a GPU communicator");
    return enqueueRecv(buffer, count, datatype, peer, stream, failed_ranks_hint,
                       failed_ranks_hint_count);
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::enqueueRecv(
    void* buffer, size_t count, DataType datatype, int peer,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_TRY(checkOpState(OpType::Recv));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_VALIDATE_ARG(buffer || bytes == 0, "recv buffer is null");
    PG_TRY(checkP2PPeer(*meta_, peer, max_group_size_, "P2P recv"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    auto completion = std::make_shared<std::promise<void>>();
    auto future = completion->get_future().share();
    auto result = std::make_unique<WorkCompletion>(std::move(future));
    p2p_proxy_->enqueueRecv(P2PProxy::RecvOp{
        .buffer_ = buffer,
        .size_ = bytes,
        .peer_rank_ = peer,
        .cuda_stream_ = stream,
        .completion_ = completion,
        .failed_ranks_hint_ = failed_ranks_hint,
    });
    return result;
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::broadcastCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int root, int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "broadcastCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::Broadcast));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "broadcast"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    }
    PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    return worker_->putTaskCpu(
        OpType::Broadcast, bytes, root, meta_, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size) {
            if (is_root) {
                std::memcpy(dst, static_cast<const char*>(send_buffer) + pos,
                            size);
            }
        },
        [=](void* src, size_t pos, size_t size) {
            std::memcpy(static_cast<char*>(recv_buffer) + pos, src, size);
        });
}

PGResult<void> MooncakeCommunicator::broadcastGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int root, cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "broadcastGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::Broadcast));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "broadcast"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    }
    PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    worker_->putTaskCuda(
        OpType::Broadcast, bytes, root, meta_, stream, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            if (is_root) {
                copyDeviceToDevice(dst,
                                   static_cast<const char*>(send_buffer) + pos,
                                   size, enqueue_stream);
            }
        },
        [=](void* src, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(static_cast<char*>(recv_buffer) + pos, src, size,
                               enqueue_stream);
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::allReduceCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "allReduceCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::AllReduce));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    PG_TRY(checkReduction(datatype, op, true));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::AllReduce, bytes, 0, meta_, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size) {
            std::memcpy(dst, static_cast<const char*>(send_buffer) + pos, size);
        },
        [=, this](void* src, size_t pos, size_t size) {
            std::memset(static_cast<char*>(recv_buffer) + pos, 0, size);
            launchReduceCpu(recv_buffer, datatype, pos, size, src, active_size,
                            op, meta_->activeRanks);
        });
}

PGResult<void> MooncakeCommunicator::allReduceGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "allReduceGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::AllReduce));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    PG_TRY(checkReduction(datatype, op, false));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::AllReduce, bytes, 0, meta_, stream, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(dst, static_cast<const char*>(send_buffer) + pos,
                               size, enqueue_stream);
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            PG_ASSERT_CUDA(
                cudaMemsetAsync(static_cast<char*>(recv_buffer) + pos, 0, size,
                                enqueue_stream));
            launchReduceKernel(recv_buffer, datatype, pos, size, src,
                               active_size, op, meta_->activeRanksDevice,
                               enqueue_stream);
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::allGatherCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "allGatherCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::AllGather));
    PG_TRY(auto send_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, send_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, send_bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::AllGather, send_bytes, 0, meta_, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size) {
            std::memcpy(dst, static_cast<const char*>(send_buffer) + pos, size);
        },
        [=, this](void* src, size_t pos, size_t size) {
            for (int peer = 0; peer < active_size; ++peer) {
                if (!meta_->activeRanks[peer]) continue;
                std::memcpy(
                    static_cast<char*>(recv_buffer) + peer * send_bytes + pos,
                    static_cast<char*>(src) + peer * size, size);
            }
        });
}

PGResult<void> MooncakeCommunicator::allGatherGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "allGatherGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::AllGather));
    PG_TRY(auto send_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, send_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, send_bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::AllGather, send_bytes, 0, meta_, stream, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(dst, static_cast<const char*>(send_buffer) + pos,
                               size, enqueue_stream);
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            for (int peer = 0; peer < active_size; ++peer) {
                if (!meta_->activeRanks[peer]) continue;
                copyDeviceToDevice(
                    static_cast<char*>(recv_buffer) + peer * send_bytes + pos,
                    static_cast<char*>(src) + peer * size, size,
                    enqueue_stream);
            }
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>>
MooncakeCommunicator::reduceScatterCpu(const void* send_buffer,
                                       void* recv_buffer, size_t count,
                                       DataType datatype, ReduceOp op,
                                       int32_t* failed_ranks_hint,
                                       size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "reduceScatterCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::ReduceScatter));
    PG_TRY(auto recv_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, recv_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, recv_bytes, "receive buffer"));
    PG_TRY(checkReduction(datatype, op, true));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::ReduceScatter, recv_bytes, 0, meta_, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size) {
            for (int peer = 0; peer < active_size; ++peer) {
                if (!meta_->activeRanks[peer]) continue;
                std::memcpy(static_cast<char*>(dst) + peer * size,
                            static_cast<const char*>(send_buffer) +
                                peer * recv_bytes + pos,
                            size);
            }
        },
        [=, this](void* src, size_t pos, size_t size) {
            std::memset(static_cast<char*>(recv_buffer) + pos, 0, size);
            launchReduceCpu(recv_buffer, datatype, pos, size, src, active_size,
                            op, meta_->activeRanks);
        });
}

PGResult<void> MooncakeCommunicator::reduceScatterGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "reduceScatterGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::ReduceScatter));
    PG_TRY(auto recv_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, recv_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, recv_bytes, "receive buffer"));
    PG_TRY(checkReduction(datatype, op, false));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::ReduceScatter, recv_bytes, 0, meta_, stream, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            for (int peer = 0; peer < active_size; ++peer) {
                if (!meta_->activeRanks[peer]) continue;
                copyDeviceToDevice(static_cast<char*>(dst) + peer * size,
                                   static_cast<const char*>(send_buffer) +
                                       peer * recv_bytes + pos,
                                   size, enqueue_stream);
            }
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            PG_ASSERT_CUDA(
                cudaMemsetAsync(static_cast<char*>(recv_buffer) + pos, 0, size,
                                enqueue_stream));
            launchReduceKernel(recv_buffer, datatype, pos, size, src,
                               active_size, op, meta_->activeRanksDevice,
                               enqueue_stream);
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::allToAllCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "allToAllCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::AllToAll));
    PG_TRY(auto peer_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, peer_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, peer_bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::AllToAll, peer_bytes, 0, meta_, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size) {
            for (int peer = 0; peer < active_size; ++peer) {
                std::memcpy(static_cast<char*>(dst) + peer * size,
                            static_cast<const char*>(send_buffer) +
                                peer * peer_bytes + pos,
                            size);
            }
        },
        [=, this](void* src, size_t pos, size_t size) {
            for (int peer = 0; peer < active_size; ++peer) {
                std::memcpy(
                    static_cast<char*>(recv_buffer) + peer * peer_bytes + pos,
                    static_cast<char*>(src) + peer * size, size);
            }
        });
}

PGResult<void> MooncakeCommunicator::allToAllGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "allToAllGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::AllToAll));
    PG_TRY(auto peer_bytes, getByteCount(count, datatype));
    PG_TRY(checkBuffer(send_buffer, peer_bytes, "send buffer"));
    PG_TRY(checkBuffer(recv_buffer, peer_bytes, "receive buffer"));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::AllToAll, peer_bytes, 0, meta_, stream, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            for (int peer = 0; peer < active_size; ++peer) {
                copyDeviceToDevice(static_cast<char*>(dst) + peer * size,
                                   static_cast<const char*>(send_buffer) +
                                       peer * peer_bytes + pos,
                                   size, enqueue_stream);
            }
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            for (int peer = 0; peer < active_size; ++peer) {
                copyDeviceToDevice(
                    static_cast<char*>(recv_buffer) + peer * peer_bytes + pos,
                    static_cast<char*>(src) + peer * size, size,
                    enqueue_stream);
            }
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::barrierCpu(
    int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "barrierCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::Barrier));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    return worker_->putTaskCpu(
        OpType::Barrier, kBarrierDummySize, 0, meta_, failed_ranks_hint,
        [](void*, size_t, size_t) {}, [](void*, size_t, size_t) {});
}

PGResult<void> MooncakeCommunicator::barrierGpu(
    cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "barrierGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::Barrier));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    worker_->putTaskCuda(
        OpType::Barrier, kBarrierDummySize, 0, meta_, stream, failed_ranks_hint,
        [](void*, size_t, size_t, cudaStream_t) {},
        [](void*, size_t, size_t, cudaStream_t) {});
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::reduceCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, int root, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "reduceCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::Reduce));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "reduce"));
    PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    }
    PG_TRY(checkReduction(datatype, op, true));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::Reduce, bytes, root, meta_, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size) {
            std::memcpy(dst, static_cast<const char*>(send_buffer) + pos, size);
        },
        [=, this](void* src, size_t pos, size_t size) {
            if (!is_root) return;
            std::memset(static_cast<char*>(recv_buffer) + pos, 0, size);
            launchReduceCpu(recv_buffer, datatype, pos, size, src, active_size,
                            op, meta_->activeRanks);
        });
}

PGResult<void> MooncakeCommunicator::reduceGpu(const void* send_buffer,
                                               void* recv_buffer, size_t count,
                                               DataType datatype, ReduceOp op,
                                               int root, cudaStream_t stream,
                                               int32_t* failed_ranks_hint,
                                               size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "reduceGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::Reduce));
    PG_TRY(auto bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "reduce"));
    PG_TRY(checkBuffer(send_buffer, bytes, "send buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(recv_buffer, bytes, "receive buffer"));
    }
    PG_TRY(checkReduction(datatype, op, false));
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::Reduce, bytes, root, meta_, stream, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(dst, static_cast<const char*>(send_buffer) + pos,
                               size, enqueue_stream);
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            if (!is_root) return;
            PG_ASSERT_CUDA(
                cudaMemsetAsync(static_cast<char*>(recv_buffer) + pos, 0, size,
                                enqueue_stream));
            launchReduceKernel(recv_buffer, datatype, pos, size, src,
                               active_size, op, meta_->activeRanksDevice,
                               enqueue_stream);
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::gatherCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int root, int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "gatherCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::Gather));
    PG_TRY(auto send_bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "gather"));
    PG_TRY(checkBuffer(send_buffer, send_bytes, "send buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(recv_buffer, send_bytes, "receive buffer"));
    }
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::Gather, send_bytes, root, meta_, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size) {
            std::memcpy(dst, static_cast<const char*>(send_buffer) + pos, size);
        },
        [=, this](void* src, size_t pos, size_t size) {
            if (!is_root) return;
            for (int peer = 0; peer < active_size; ++peer) {
                std::memcpy(
                    static_cast<char*>(recv_buffer) + peer * send_bytes + pos,
                    static_cast<char*>(src) + peer * size, size);
            }
        });
}

PGResult<void> MooncakeCommunicator::gatherGpu(const void* send_buffer,
                                               void* recv_buffer, size_t count,
                                               DataType datatype, int root,
                                               cudaStream_t stream,
                                               int32_t* failed_ranks_hint,
                                               size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "gatherGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::Gather));
    PG_TRY(auto send_bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "gather"));
    PG_TRY(checkBuffer(send_buffer, send_bytes, "send buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(recv_buffer, send_bytes, "receive buffer"));
    }
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::Gather, send_bytes, root, meta_, stream, failed_ranks_hint,
        [=](void* dst, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(dst, static_cast<const char*>(send_buffer) + pos,
                               size, enqueue_stream);
        },
        [=, this](void* src, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            if (!is_root) return;
            for (int peer = 0; peer < active_size; ++peer) {
                copyDeviceToDevice(
                    static_cast<char*>(recv_buffer) + peer * send_bytes + pos,
                    static_cast<char*>(src) + peer * size, size,
                    enqueue_stream);
            }
        });
    return {};
}

PGResult<std::unique_ptr<WorkCompletion>> MooncakeCommunicator::scatterCpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int root, int32_t* failed_ranks_hint, size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(is_cpu_, "scatterCpu requires a CPU communicator");
    PG_TRY(checkOpState(OpType::Scatter));
    PG_TRY(auto recv_bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "scatter"));
    PG_TRY(checkBuffer(recv_buffer, recv_bytes, "receive buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(send_buffer, recv_bytes, "send buffer"));
    }
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    return worker_->putTaskCpu(
        OpType::Scatter, recv_bytes, root, meta_, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size) {
            if (!is_root) return;
            for (int peer = 0; peer < active_size; ++peer) {
                std::memcpy(static_cast<char*>(dst) + peer * size,
                            static_cast<const char*>(send_buffer) +
                                peer * recv_bytes + pos,
                            size);
            }
        },
        [=](void* src, size_t pos, size_t size) {
            std::memcpy(static_cast<char*>(recv_buffer) + pos, src, size);
        });
}

PGResult<void> MooncakeCommunicator::scatterGpu(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    int root, cudaStream_t stream, int32_t* failed_ranks_hint,
    size_t failed_ranks_hint_count) {
    PG_VALIDATE_STATE(!is_cpu_, "scatterGpu requires a GPU communicator");
    PG_TRY(checkOpState(OpType::Scatter));
    PG_TRY(auto recv_bytes, getByteCount(count, datatype));
    PG_TRY(checkRoot(root, max_group_size_, "scatter"));
    PG_TRY(checkBuffer(recv_buffer, recv_bytes, "receive buffer"));
    const bool is_root = root == rank_;
    if (is_root) {
        PG_TRY(checkBuffer(send_buffer, recv_bytes, "send buffer"));
    }
    PG_TRY(
        initializeFailedRanksHint(failed_ranks_hint, failed_ranks_hint_count));
    const int active_size = getSize();
    worker_->putTaskCuda(
        OpType::Scatter, recv_bytes, root, meta_, stream, failed_ranks_hint,
        [=, this](void* dst, size_t pos, size_t size,
                  cudaStream_t enqueue_stream) {
            if (!is_root) return;
            for (int peer = 0; peer < active_size; ++peer) {
                copyDeviceToDevice(static_cast<char*>(dst) + peer * size,
                                   static_cast<const char*>(send_buffer) +
                                       peer * recv_bytes + pos,
                                   size, enqueue_stream);
            }
        },
        [=](void* src, size_t pos, size_t size, cudaStream_t enqueue_stream) {
            copyDeviceToDevice(static_cast<char*>(recv_buffer) + pos, src, size,
                               enqueue_stream);
        });
    return {};
}

PGResult<void> MooncakeCommunicator::shutdown() {
    if (is_shutdown_) return {};
    std::unique_ptr<GpuDeviceGuard> device_guard;
    const bool has_device_state =
        !is_cpu_ &&
        (active_ranks_mirror_stream_.has_value() || send_buffer_[0] ||
         recv_buffer_[0] || worker_ || p2p_proxy_ || meta_);
    if (has_device_state) {
        device_guard = std::make_unique<GpuDeviceGuard>(device_index_);
    }
    is_shutdown_ = true;
    // Remove this communicator from AgentHost's callback lookup before teardown
    // so a concurrent ViewUpdate cannot call into it. Keep the group registered
    // locally and at the Coordinator while worker tasks are draining because
    // their failure path may still call syncAfterFailure().
    if (isValidGroup()) agent_.detachCommunicator(meta_->group_id);

    // If we encounter any hung operations, don't release resources to avoid a
    // potential crash. Instead, allow those resources to leak and rely on the
    // OS to reclaim them later.
    bool has_hung_operation = false;

    // Phase 1: Drain P2P tasks.
    if (p2p_device_worker_ && p2p_proxy_) {
        p2p_device_worker_->removeProxy(p2p_proxy_);
        has_hung_operation |= !p2p_proxy_->drainTasks();
    }
    // Phase 2: Drain collective tasks for this communicator.
    if (worker_ && meta_) {
        has_hung_operation |= !worker_->drainTasks(meta_.get());
    }
    // Phase 3: Device synchronization.
    if (has_device_state && !has_hung_operation) cudaDeviceSynchronize();

    // Phase 4: Release resources.
    if (has_hung_operation && p2p_proxy_) p2p_proxy_->abandonResources();

    if (!has_hung_operation && meta_) {
        for (size_t index = 0; index < 2; ++index) {
            context_.engine->unregisterLocalMemory(
                cpu_sync_send_region_[index]);
            context_.engine->unregisterLocalMemory(
                cpu_sync_recv_region_[index]);
            context_.engine->unregisterLocalMemory(send_buffer_[index]);
            context_.engine->unregisterLocalMemory(recv_buffer_[index]);
            delete[] cpu_sync_send_region_[index];
            delete[] cpu_sync_recv_region_[index];
            if (is_cpu_) {
                std::free(send_buffer_[index]);
                std::free(recv_buffer_[index]);
            } else {
                cudaFree(send_buffer_[index]);
                cudaFree(recv_buffer_[index]);
            }
        }
        delete[] meta_->maybeActivatable;
        if (is_cpu_) {
            delete[] meta_->activeRanks;
        } else {
            cudaFreeHost(meta_->activeRanks);
        }
        meta_->activeRanks = nullptr;
        meta_->activeRanksDevice = nullptr;
        meta_->maybeActivatable = nullptr;
    }
    // Prevent zombie P2PProxy workers from dereferencing this communicator
    // after destruction. Must happen after drainTasks so in-flight failures can
    // still be reported during shutdown.
    if (meta_) meta_->communicator = nullptr;

    // The data-plane teardown has finished. Remove the group from the local
    // Agent and notify the Coordinator that this rank has left it.
    if (isValidGroup()) {
        PG_TRY(agent_.unregisterGroup(meta_->group_id));
    }
    return {};
}

std::vector<int32_t> MooncakeCommunicator::getActiveRanks() const {
    std::vector<int32_t> result(max_group_size_, 0);
    if (!meta_ || !meta_->activeRanks) return result;
    for (int index = 0; index < max_group_size_; ++index) {
        result[index] = meta_->activeRanks[index] ? 1 : 0;
    }
    return result;
}

void MooncakeCommunicator::syncActiveRanksMirror() const {
    if (!active_ranks_mirror_) return;
    // The mirror is InGroupRank-indexed, in the same order as the caller-owned
    // storage.
    auto active_ranks = getActiveRanks();
    const size_t bytes = max_group_size_ * sizeof(int32_t);
    if (active_ranks_mirror_is_device_) {
        const GpuDeviceGuard device_guard(active_ranks_mirror_device_index_);
        PG_ASSERT_CUDA(cudaMemcpyAsync(
            active_ranks_mirror_, active_ranks.data(), bytes,
            cudaMemcpyHostToDevice, active_ranks_mirror_stream_.value().get()));
    } else {
        std::memcpy(active_ranks_mirror_, active_ranks.data(), bytes);
    }
}

int MooncakeCommunicator::getNumSyncedRanks() const {
    if (!meta_ || !meta_->maybeActivatable) return 0;
    int count = 0;
    for (int index = 0; index < max_group_size_; ++index) {
        if (meta_->maybeActivatable[index]) ++count;
    }
    return count;
}

PGResult<void> MooncakeCommunicator::checkValidGroup(
    const char* operation) const {
    PG_VALIDATE_STATE(!is_shutdown_, "communicator is shut down");
    if (!isValidGroup()) {
        return makePGError(
            PGErrorCode::NotSupported,
            std::string(operation) +
                " is unavailable because this communicator is invalid");
    }
    return {};
}

PGResult<std::vector<bool>> MooncakeCommunicator::getPeerState(
    const std::vector<int>& ranks) const {
    PG_TRY(checkValidGroup("getPeerState"));
    std::vector<bool> result;
    result.reserve(ranks.size());
    for (const int rank : ranks) {
        PG_VALIDATE_ARG(rank >= 0 && rank < max_group_size_,
                        "peer rank is out of range");
        result.push_back(meta_->maybeActivatable[rank]);
    }
    return result;
}

PGResult<ProposeViewUpdateResponse> MooncakeCommunicator::activateRanks(
    const std::vector<int>& ranks) {
    PG_TRY(checkValidGroup("activateRanks"));
    for (const int rank : ranks) {
        PG_VALIDATE_ARG(rank >= 0 && rank < max_group_size_,
                        "rank to activate is out of range");
    }
    std::vector<InGroupRank> local_ranks(ranks.begin(), ranks.end());
    auto result = agent_.proposeActivate(meta_->group_id, local_ranks);
    if (!result.has_value()) {
        return makePGError(std::move(result).error());
    }
    if (result.value().status == ProposalStatus::Rejected) {
        LOG(WARNING) << "MooncakeCommunicator: activateRanks rejected: "
                     << result.value().reject_reason;
    }
    return result;
}

PGResult<ProposeViewUpdateResponse> MooncakeCommunicator::deactivateRanks(
    const std::vector<int>& ranks) {
    PG_TRY(checkValidGroup("deactivateRanks"));
    for (const int rank : ranks) {
        PG_VALIDATE_ARG(rank >= 0 && rank < max_group_size_,
                        "rank to deactivate is out of range");
    }
    std::vector<InGroupRank> local_ranks(ranks.begin(), ranks.end());
    auto result = agent_.proposeDeactivate(meta_->group_id, local_ranks);
    if (!result.has_value()) {
        return makePGError(std::move(result).error());
    }
    if (result.value().status == ProposalStatus::Rejected) {
        LOG(WARNING) << "MooncakeCommunicator: deactivateRanks rejected: "
                     << result.value().reject_reason;
    }
    return result;
}

PGResult<void> MooncakeCommunicator::joinGroup() {
    PG_TRY(checkValidGroup("joinGroup"));
    auto mode = meta_->extensionMode.load(std::memory_order_acquire);
    PG_VALIDATE_STATE(
        mode == CollectiveExtensionState::Isolated,
        "joinGroup may only be called once on an isolated joining "
        "communicator");
    // Stop admitting isolated collectives before advertising readiness.
    meta_->extensionMode.store(CollectiveExtensionState::Quiescing,
                               std::memory_order_release);
    if (!worker_->drainTasks(meta_.get())) {
        return makePGError(
            PGErrorCode::Timeout,
            "timed out draining join preparation collectives for rank " +
                std::to_string(meta_->globalRank));
    }
    PG_TRY(agent_.confirmReadyForActivation(meta_->group_id));
    // Block until the Coordinator activates this rank in the group.
    PG_TRY(agent_.waitUntilRankActive(meta_->group_id, meta_->globalRank,
                                      std::chrono::seconds(300)));
    const bool normal_and_active =
        meta_->extensionMode.load(std::memory_order_acquire) ==
            CollectiveExtensionState::Normal &&
        meta_->activeRanks[rank_];
    PG_ASSERT(normal_and_active, "Bad waitUntilRankActive");
    LOG(INFO) << "joinGroup rank=" << meta_->globalRank
              << " group=" << meta_->group_id << " activated";
    return {};
}

uint64_t MooncakeCommunicator::getCurrentEpoch() const {
    return meta_ ? meta_->epoch.load(std::memory_order_acquire) : 0;
}

PGResult<SyncAfterFailureResponse> MooncakeCommunicator::syncAfterFailure() {
    PG_TRY(checkValidGroup("syncAfterFailure"));
    return agent_.syncAfterFailure(meta_->group_id);
}

void MooncakeCommunicator::applyViewUpdate(
    const GroupView& view, const std::vector<RankState>& rank_states,
    const std::vector<uint64_t>& rank_epochs,
    const std::vector<bool>& activatable) {
    if (!meta_) return;

    // Ignore stale views that arrive out of order
    auto current_epoch = meta_->epoch.load(std::memory_order_acquire);
    if (view.epoch < current_epoch) {
        return;
    }

    bool epoch_changed = current_epoch != view.epoch;

    // An authoritative view in which self is Active is the common commit point
    // for enabling normal collective execution:
    //
    //   founding ranks: Isolated  -> Normal
    //   joining ranks:  Quiescing -> Normal
    //
    // A non-Active view deliberately does not determine the local mode. A new
    // joiner must remain Isolated until joinGroup is called; a joiner awaiting
    // activation must remain Quiescing; and an auto-deactivated communicator
    // must remain Normal so its inactive self bit makes the next collective
    // fail fast.
    auto mode = meta_->extensionMode.load(std::memory_order_acquire);
    auto next_mode = mode;
    if (view.members[meta_->globalRank].isActive()) {
        next_mode = CollectiveExtensionState::Normal;
    }

    PG_ASSERT(
        static_cast<int32_t>(view.rank_order.size()) <= meta_->maxGroupSize,
        "Bad group view");

    // Preserve stable in-group rank slots: activeSize is the upper bound of the
    // active rank space, not the number of set bits. For example, an active
    // mask of [true, false, true] has activeSize == 3.
    int active_size = 0;
    for (size_t local_rank = 0; local_rank < view.rank_order.size();
         ++local_rank) {
        const auto global_rank = view.rank_order[local_rank];
        if (view.members[global_rank].isActive()) {
            active_size = static_cast<int>(local_rank) + 1;
        }
    }

    std::vector<bool> previous_active_ranks(meta_->maxGroupSize);
    for (int local_rank = 0; local_rank < meta_->maxGroupSize; ++local_rank) {
        previous_active_ranks[local_rank] = meta_->activeRanks[local_rank];
    }

    // The execution mode determines the effective active ranks consumed by
    // kernels. Isolated and Quiescing use a local-only mask; Normal follows the
    // Coordinator's committed membership view.
    switch (next_mode) {
        case CollectiveExtensionState::Isolated:
        case CollectiveExtensionState::Quiescing:
            for (int local_rank = 0; local_rank < meta_->maxGroupSize;
                 ++local_rank) {
                meta_->activeRanks[local_rank] = local_rank == rank_;
            }
            break;
        case CollectiveExtensionState::Normal:
            for (int local_rank = 0; local_rank < meta_->maxGroupSize;
                 ++local_rank) {
                meta_->activeRanks[local_rank] = false;
            }
            for (size_t local_rank = 0; local_rank < view.rank_order.size();
                 ++local_rank) {
                const auto global_rank = view.rank_order[local_rank];
                meta_->activeRanks[local_rank] =
                    view.members[global_rank].isActive();
            }
            break;
    }

    // Only a change in execution mode or effective participants starts a new
    // collective taskCount. Endpoint, AwaitingActivation updates, ... keep the
    // current taskCount even though they advance the view epoch.
    bool reset_task_count = next_mode != mode;
    for (int local_rank = 0; local_rank < meta_->maxGroupSize; ++local_rank) {
        reset_task_count |=
            previous_active_ranks[local_rank] != meta_->activeRanks[local_rank];
    }
    if (reset_task_count) meta_->taskCount = 0;

    // Rank order and endpoint metadata.
    for (size_t local_rank = 0; local_rank < view.rank_order.size();
         ++local_rank) {
        // rank order
        meta_->rank_order[local_rank] = view.rank_order[local_rank];
        const auto global_rank = view.rank_order[local_rank];

        const auto& member = view.members[global_rank];
        if (member.endpoint.has_value()) {
            meta_->segmentInfos[local_rank] = *member.endpoint;
        }
    }

    // Rank states
    for (size_t i = 0; i < rank_states.size(); ++i) {
        meta_->rankStates[i] = rank_states[i];
    }
    for (size_t i = 0; i < rank_epochs.size(); ++i) {
        meta_->rankEpochs[i] = rank_epochs[i];
    }

    // Best-effort Activatable
    for (size_t i = 0; i < activatable.size(); ++i) {
        meta_->maybeActivatable[i] = activatable[i];
    }

    // Keep the caller-visible active-ranks mirror in sync with the view.
    // FIXME: potential deadlock?
    syncActiveRanksMirror();

    // Publish the rank-space extent after the corresponding data-plane state.
    // getSize() reads this from the application thread.
    meta_->activeSize.store(active_size, std::memory_order_release);

    // Publish epoch AFTER all data-plane state (activeRanks, segmentInfos,
    // etc.) is updated.  This ensures that a thread observing the new epoch via
    // getCurrentEpoch() (acquire) sees the complete membership state.
    if (epoch_changed) {
        meta_->epoch.store(view.epoch, std::memory_order_release);
    }

    if (next_mode != mode) {
        meta_->extensionMode.store(next_mode, std::memory_order_release);
    }
}

void MooncakeCommunicator::onPeerLinkReset(InGroupRank peer) {
    if (is_shutdown_) return;
    if (p2p_proxy_) p2p_proxy_->resetPeerState(peer);
    if (peer >= 0 && peer < max_group_size_) {
        meta_->segmentIDs[peer] = static_cast<TransferMetadata::SegmentID>(-1);
    }
}

void MooncakeCommunicator::refreshSegmentID(InGroupRank local) {
    if (local < 0 || local >= max_group_size_) return;
    const auto handle =
        context_.link_manager.resolvePeer(meta_->rank_order[local]);
    meta_->segmentIDs[local] =
        handle ? *handle : static_cast<TransferMetadata::SegmentID>(-1);
}

GroupEndpointPublication MooncakeCommunicator::buildEndpointMetadata() const {
    return GroupEndpointPublication{
        .group_id = meta_->group_id,
        .endpoint_info = meta_->segmentInfos[meta_->rank]};
}

}  // namespace mooncake
