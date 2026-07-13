#include <ATen/cuda/CUDAContext.h>
#include <ATen/ops/empty.h>
#include <cuda_alike.h>
#include <torch/torch.h>
#include <iostream>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <p2p_proxy.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>
#include <unistd.h>
#include "control_plane/types.h"
#include "memory_location.h"
#include "mooncake_worker.cuh"
#include "pg_utils.h"
#include "control_plane/agent_host.h"
#include "control_plane/link_manager.h"

namespace mooncake {

constexpr const char* REGISTER_BUFFER_ERROR_MSG =
    "Failed to register local memory.";
constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SPARSE_ERROR_MSG = "Sparse op not supported.";
constexpr int kBarrierDummyTensorSize = 1;

// PyTorch may reuse the same logical group_id (e.g. "0") when a process
// group is destroyed and recreated.  The Coordinator keys group state by
// group_id, so reusing an id causes stale membership (e.g. Left) from the
// previous incarnation to leak into the new group.
//
// We therefore generate a fresh Mooncake-specific group_id for every backend
// instance.  The in-group leader (group_rank == 0) generates the id and
// publishes it on the group-local store; the other ranks wait for the key.
// A per-logical-group counter is used to build a unique store key for each
// init round, avoiding stale reads when the same store is reused.
static std::string makeMooncakeGroupId(c10d::DistributedBackendOptions& opts) {
    const std::string& base = opts.group_id;
    if (base.empty()) {
        return base;
    }

    static std::mutex mu;
    static std::unordered_map<std::string, int64_t> counter;
    int64_t seq;
    {
        std::lock_guard<std::mutex> lk(mu);
        seq = ++counter[base];
    }

    const std::string sync_key =
        "mooncake_group_id_" + base + "_" + std::to_string(seq);

    if (opts.group_rank == 0) {
        auto now = std::chrono::steady_clock::now().time_since_epoch();
        auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(now).count();
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        std::string unique = base + "_" + std::to_string(us) + "_" +
                             std::to_string(getpid()) + "_" +
                             std::to_string(dis(gen));

        opts.store->set(sync_key, unique);
    }

    opts.store->wait({sync_key});
    std::string unique = opts.store->get_to_str(sync_key);
    return unique;
}

/**
 * @brief Initialize Mooncake backend state from the PyTorch process-group
 * information and optional Mooncake-specific options.
 */
MooncakeBackend::MooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackendOptions> options, AgentInterface& agent,
    MooncakeProcessContext& ctx, bool isCpu)
    : ProcessGroup(distBackendOpts.store, distBackendOpts.group_rank,
                   distBackendOpts.group_size),
      ctx_(ctx),
      options_(std::move(options)),
      isCpu_(isCpu),
      agent_(agent) {
    const int rank = distBackendOpts.group_rank;
    const int size = distBackendOpts.group_size;
    const auto& globalRanks = distBackendOpts.global_ranks_in_group;
    const int max_group_size = (options_ && options_->maxGroupSize_ > 0)
                                   ? options_->maxGroupSize_
                                   : size;

    TORCH_CHECK(max_group_size >= 0 &&
                    static_cast<size_t>(max_group_size) <= kMaxNumRanks,
                "max_group_size out of range");
    TORCH_CHECK(max_group_size >= size,
                "max_group_size must be >= process group size");
    TORCH_CHECK(rank >= 0 && rank < max_group_size, "rank out of valid range");

    max_group_size_ = max_group_size;

    // Build rank order from global_ranks_in_group.
    // rank order is a mapping from in-group rank to global rank.
    std::vector<GlobalRank> initial_rank_order;
    initial_rank_order.reserve(size);
    if (globalRanks.size() == static_cast<size_t>(size)) {
        for (int i = 0; i < size; ++i) {
            initial_rank_order.push_back(globalRanks[i]);
        }
    } else {
        // fallback with identical mapping
        initial_rank_order.resize(size);
        std::iota(initial_rank_order.begin(), initial_rank_order.end(), 0);
    }

    // Memory location for device specific buffers
    // always kWildcardLocation for cpu backend
    std::string location = kWildcardLocation;
    if (!isCpu) {
        int deviceCount = 0;
        cudaError_t err = cudaGetDeviceCount(&deviceCount);
        if (err == cudaSuccess && deviceCount != 0) {
            int deviceId_;
            err = cudaGetDevice(&deviceId_);
            TORCH_CHECK(!err, c10::str("Failed to get device id"));
            location = GPU_PREFIX + std::to_string(deviceId_);
        }
    }

    // Register buffers
    if (isCpu) {
        for (size_t i = 0; i < 2; i++) {
            send_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(send_buffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            int rc = ctx_.engine->registerLocalMemory(send_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            recv_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(recv_buffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            int rc = ctx_.engine->registerLocalMemory(recv_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

#ifdef USE_MACA
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err,
                        c10::str("Failed to allocate MACA GPU send buffer"));

            int rc = ctx_.engine->registerLocalMemory(send_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err,
                        c10::str("Failed to allocate MACA GPU recv buffer"));

            int rc = ctx_.engine->registerLocalMemory(recv_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
#else
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = ctx_.engine->registerLocalMemory(send_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = ctx_.engine->registerLocalMemory(recv_buffer_[i],
                                                      kBufferSize, location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
#endif
    }

    // Register CPU sync regions
    TORCH_CHECK(static_cast<size_t>(size) <= kMaxNumRanks,
                "The number of ranks exceeds the limit.");
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks]{};
        int rc = ctx_.engine->registerLocalMemory(
            cpu_sync_send_region_[i], kMaxNumRanks * sizeof(int32_t),
            kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks]{};
        int rc = ctx_.engine->registerLocalMemory(
            cpu_sync_recv_region_[i], kMaxNumRanks * sizeof(int32_t),
            kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    auto& dev_worker_mgr = ctx_.p2p_device_worker_manager;
    int cuda_device_index = isCpu_ ? -1 : at::cuda::current_device();

    if (isCpu_)
        p2p_device_worker_ = dev_worker_mgr.getCPUWorker(ctx_.engine);
    else
        p2p_device_worker_ =
            dev_worker_mgr.getCUDAWorker(cuda_device_index, ctx_.engine);

    auto& worker_mgr = ctx_.worker_manager;
    if (isCpu_)
        worker_ = worker_mgr.GetCPUWorker();
    else
        worker_ = worker_mgr.GetCUDAWorker(cuda_device_index);
    if (!isCpu_) {
        preloadReduceKernels();
    }
    worker_->Start();

    p2p_proxy_ = std::make_shared<P2PProxy>(
        ctx_.engine, P2PProxy::Options{
                         .is_cpu = isCpu_,
                         .rank = rank_,
                         .size = max_group_size_,
                         .cuda_device_index = cuda_device_index,
                         .p2p_timeout_us = &ctx_.p2p_timeout_us,
                     });
    p2p_device_worker_->registerProxy(p2p_proxy_);

    meta_ = std::make_shared<TransferGroupMeta>();
    for (int i = 0; i < kMaxNumRanks; ++i) {
        meta_->segmentIDs[i] = static_cast<TransferMetadata::SegmentID>(-1);
    }
    meta_->rank = rank;
    meta_->globalRank = initial_rank_order[rank];
    for (int32_t i = 0; i < max_group_size_; ++i) {
        // initial_rank_order only has `size` entries; for remaining
        // extension slots default to identity so that in-group rank i
        // maps to global rank i until applyViewUpdate overwrites it.
        if (i < size) {
            meta_->rank_order[i] = initial_rank_order[i];
        } else {
            meta_->rank_order[i] = static_cast<GlobalRank>(i);
        }
    }
    meta_->maxGroupSize = max_group_size_;  // slot capacity
    meta_->taskCount = 0;
    meta_->collectiveTimeoutUs = &ctx_.collective_timeout_us;
    meta_->engine = ctx_.engine;
    meta_->backend = this;
    p2p_proxy_->bindMeta(meta_);

    // active ranks will be filled by applyViewUpdate, so here we just
    // allocate them without initialization.
    if (isCpu) {
        meta_->activeRanks = new bool[max_group_size_]{};
    } else {
        cudaHostAlloc(&meta_->activeRanks, max_group_size_ * sizeof(bool),
                      cudaHostAllocMapped);
        cudaHostGetDevicePointer(&meta_->activeRanksDevice, meta_->activeRanks,
                                 0);
    }
    // Use user-provided tensor memory if available (content is overwritten
    // by the Coordinator via applyViewUpdate).
    if (options_ && options_->activeRanks_.defined()) {
        meta_->activeRanksTensor = options_->activeRanks_;
    } else {
        meta_->activeRanksTensor = at::empty(
            {max_group_size_}, torch::dtype(torch::kInt32)
                                   .device(isCpu ? torch::kCPU : torch::kCUDA));
    }

    // Initial local endpoint info
    meta_->segmentInfos[rank] = GroupEndpointInfo{
        .send_buffer = {(uint64_t)send_buffer_[0], (uint64_t)send_buffer_[1]},
        .recv_buffer = {(uint64_t)recv_buffer_[0], (uint64_t)recv_buffer_[1]},
        .send_sync = {(uint64_t)cpu_sync_send_region_[0],
                      (uint64_t)cpu_sync_send_region_[1]},
        .recv_sync = {(uint64_t)cpu_sync_recv_region_[0],
                      (uint64_t)cpu_sync_recv_region_[1]},
        .p2p_credit_region = (uint64_t)p2p_proxy_->credit_region(),
        .p2p_ack_region = (uint64_t)p2p_proxy_->ack_region(),
    };

    TORCH_CHECK(!distBackendOpts.group_id.empty(),
                "MooncakeBackend: distBackendOpts.group_id must not be empty");
    std::string mooncake_group_id = makeMooncakeGroupId(distBackendOpts);
    TORCH_CHECK(!mooncake_group_id.empty(),
                "MooncakeBackend: mooncake_group_id must not be empty");
    meta_->group_id = std::move(mooncake_group_id);

    // Register a lightweight Backend shim so that PyTorch's P2P dispatch path
    // (batch_isend_irecv → _get_backend → getBackend) can find a registered
    // Backend for this ProcessGroup.  The shim delegates send/recv back to us.
    auto deviceType = isCpu ? c10::DeviceType::CPU : c10::DeviceType::CUDA;
    auto shim = c10::make_intrusive<MooncakeP2PShim>(this);
    setBackend(deviceType, BackendType::CUSTOM, shim);
#ifndef MOONCAKE_EP_USE_MUSA
    setDefaultBackend(BackendType::CUSTOM);
#endif

    // Control Plane Initialization

    // Wait for Agent registration
    if (!agent_.waitUntilRegistered(std::chrono::seconds(30))) {
        throw std::runtime_error(
            "MooncakeBackend: Agent registration timed out");
    }

    // Register this group with the Agent, publish local endpoint, and block
    // until the Coordinator says ready.
    GroupView initial_group;
    initial_group.group_id = meta_->group_id;
    initial_group.rank_order = std::move(initial_rank_order);
    initial_group.members.resize(ctx_.max_world_size);
    for (GlobalRank r : initial_group.rank_order) {
        initial_group.members[r].status = GroupMemberStatus::Active;
    }
    bool auto_deactivate = [&]() -> bool {
        if (options_) return options_->autoDeactivateOnFailure_;
        if (const char* val =
                std::getenv("MOONCAKE_PG_AUTO_DEACTIVATE_ON_FAILURE"))
            return (std::string(val) == "1" || std::string(val) == "true");
        return true;
    }();
    bool auto_sync_on_failure = [&]() -> bool {
        if (options_) return options_->autoSyncOnFailure_;
        if (const char* val = std::getenv("MOONCAKE_PG_AUTO_SYNC_ON_FAILURE"))
            return (std::string(val) == "1" || std::string(val) == "true");
        return true;
    }();

    // auto_sync_on_failure requires auto_deactivate_on_failure.
    TORCH_CHECK(
        !auto_sync_on_failure || auto_deactivate,
        "auto_sync_on_failure requires auto_deactivate_on_failure=true");

    meta_->autoSyncOnFailure = auto_sync_on_failure;

    // Register group is synchronous.  Pass `this` as a raw pointer — the
    // backend's lifetime is managed by the intrusive_ptr returned from
    // make_intrusive; backends_ only needs a non-owning lookup key.
    agent_.registerGroup(initial_group, auto_deactivate, this);

    agent_.publishLocalEndpoint(buildEndpointMetadata());

    auto view =
        agent_.waitUntilGroupReady(meta_->group_id, std::chrono::seconds(30));

    applyViewUpdate(view);

    // Initialize all peer segment IDs from the LinkManager.
    // Subsequent updates (endpoint changes, disconnects) are handled by
    // the NotifyLinkRefreshed effect.
    for (int local = 0; local < meta_->maxGroupSize; ++local) {
        refreshSegmentID(local);
    }
}

MooncakeBackend::~MooncakeBackend() {
    // Ensure this backend is removed from backends_ BEFORE shutdown.
    // Blocks until the executor thread has erased the raw pointer so that
    // in-flight ViewUpdates cannot find a backend with freed resources.
    if (meta_) {
        agent_.unregisterGroup(meta_->group_id);
    }
    shutdown();
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

// ---- MooncakeP2PShim implementation ----

MooncakeP2PShim::MooncakeP2PShim(MooncakeBackend* owner)
    : Backend(owner->getRank(), owner->getSize()), owner_(owner) {}

const std::string MooncakeP2PShim::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeP2PShim::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    return owner_->send(tensors, dstRank, tag);
}

c10::intrusive_ptr<c10d::Work> MooncakeP2PShim::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    return owner_->recv(tensors, srcRank, tag);
}

c10::intrusive_ptr<c10d::Work> MooncakeP2PShim::recvAnysource(
    std::vector<at::Tensor>& tensors, int tag) {
    // MooncakeBackend doesn't implement recvAnysource; fall back to
    // the base class which will raise a clear error.
    return ::c10d::Backend::recvAnysource(tensors, tag);
}

c10::intrusive_ptr<c10d::Work> MooncakeP2PShim::barrier(
    const c10d::BarrierOptions& opts) {
    return owner_->barrier(opts);
}

void MooncakeBackend::prepareOp(c10d::OpType op) const {
    TORCH_CHECK(agent_.getRankState(meta_->globalRank) != RankState::Offline,
                "Rank ", meta_->globalRank,
                " is Offline. Cannot perform operations.");
    // P2P operations don't require the rank to be active in the group.
    if (op != c10d::OpType::SEND && op != c10d::OpType::RECV) {
        TORCH_CHECK(agent_.isRankActive(meta_->group_id,
                                        static_cast<InGroupRank>(rank_)),
                    "Rank ", meta_->globalRank,
                    " is not active in this group. "
                    "Cannot perform collective operations.");
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(dstRank >= 0 && dstRank < meta_->maxGroupSize,
                "P2P send: dstRank out of range.");

    prepareOp(c10d::OpType::SEND);

    auto contiguous = tensor.contiguous();
    auto status = std::make_shared<std::atomic<P2PProxy::OpStatus>>(
        P2PProxy::OpStatus::kPending);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(contiguous.device().index());
        stream = current_stream.stream();
    }

    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);

    TORCH_CHECK(p2p_proxy_, "P2P send proxy is not initialized.");
    p2p_proxy_->enqueueSend(P2PProxy::SendOp{
        .tensor_ = std::move(contiguous),
        .peer_rank_ = dstRank,
        .cuda_stream_ = stream,
        .status_ = status,
        .failed_ranks_hint_ = failed_ranks.data(),
    });

    return c10::make_intrusive<MooncakeP2PWork>(status,
                                                std::move(failed_ranks));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(srcRank >= 0 && srcRank < meta_->maxGroupSize,
                "P2P recv: srcRank out of range.");

    prepareOp(c10d::OpType::RECV);

    auto target = tensor.is_contiguous() ? tensor : tensor.contiguous();
    auto status = std::make_shared<std::atomic<P2PProxy::OpStatus>>(
        P2PProxy::OpStatus::kPending);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(target.device().index());
        stream = current_stream.stream();
    }

    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);

    TORCH_CHECK(p2p_proxy_, "P2P recv proxy is not initialized.");
    p2p_proxy_->enqueueRecv(P2PProxy::RecvOp{
        .tensor_ = target,
        .original_tensor_ = tensor,
        .peer_rank_ = srcRank,
        .cuda_stream_ = stream,
        .status_ = status,
        .failed_ranks_hint_ = failed_ranks.data(),
    });

    return c10::make_intrusive<MooncakeP2PWork>(status,
                                                std::move(failed_ranks));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::broadcast(
    std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    prepareOp(c10d::OpType::BROADCAST);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::BROADCAST, tensorSize, root, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                memcpy((char*)tensor.data_ptr() + pos, src, realSize);
            });
    } else {
        at::cuda::CUDAStream stream =
            at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::BROADCAST, tensorSize, root, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                if (isRoot) {
                    cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos,
                                    realSize, cudaMemcpyDeviceToDevice,
                                    enq_stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync((char*)tensor.data_ptr() + pos, src, realSize,
                                cudaMemcpyDeviceToDevice, enq_stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(opts.sparseIndices == std::nullopt, SPARSE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    prepareOp(c10d::OpType::ALLREDUCE);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        auto numRanks = meta_->maxGroupSize;
        return worker_->putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                memset((char*)tensor.data_ptr() + pos, 0, realSize);
                launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_->activeRanks);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, enq_stream);
            },
            [=, this](void* src, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                enq_stream);
                launchReduceKernel(tensor, pos, realSize, src,
                                   meta_->maxGroupSize, opts.reduceOp,
                                   meta_->activeRanksDevice, enq_stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions& opts) {
    TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto inputTensor = inputTensors.back();
    auto outputTensors_ = outputTensors.back();
    size_t tensorSize = inputTensor.numel() * inputTensor.element_size();
    prepareOp(c10d::OpType::ALLGATHER);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::ALLGATHER, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputTensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    memcpy((char*)outputTensors_[j].data_ptr() + pos,
                           (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::ALLGATHER, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, enq_stream);
            },
            [=](void* src, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    cudaMemcpyAsync((char*)outputTensors_[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToDevice, enq_stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions& opts) {
    size_t tensorSize = inputBuffer.numel() * inputBuffer.element_size();
    prepareOp(c10d::OpType::_ALLGATHER_BASE);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputBuffer.data_ptr() + pos, realSize);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                for (int j = 0; j < meta_->maxGroupSize; ++j) {
                    if (!meta_->activeRanks[j]) continue;
                    memcpy(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync(dst, (char*)inputBuffer.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, enq_stream);
            },
            [=, this](void* src, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                for (int j = 0; j < meta_->maxGroupSize; ++j) {
                    if (!meta_->activeRanks[j]) continue;
                    cudaMemcpyAsync(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize,
                        cudaMemcpyDeviceToDevice, enq_stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    size_t tensorSize = outputBuffer.numel() * outputBuffer.element_size();
    prepareOp(c10d::OpType::_REDUCE_SCATTER_BASE);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        auto numRanks = meta_->maxGroupSize;
        return worker_->putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=, this](void* dst, size_t pos, size_t realSize) {
                for (int j = 0; j < meta_->maxGroupSize; ++j) {
                    if (!meta_->activeRanks[j]) continue;
                    memcpy((char*)dst + j * realSize,
                           (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                           realSize);
                }
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                memset((char*)outputBuffer.data_ptr() + pos, 0, realSize);
                launchReduceCpu(outputBuffer, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_->activeRanks);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=, this](void* dst, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                for (int j = 0; j < meta_->maxGroupSize; ++j) {
                    if (!meta_->activeRanks[j]) continue;
                    cudaMemcpyAsync(
                        (char*)dst + j * realSize,
                        (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                        realSize, cudaMemcpyDeviceToDevice, enq_stream);
                }
            },
            [=, this](void* src, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                cudaMemsetAsync((char*)outputBuffer.data_ptr() + pos, 0,
                                realSize, enq_stream);
                launchReduceKernel(outputBuffer, pos, realSize, src,
                                   meta_->maxGroupSize, opts.reduceOp,
                                   meta_->activeRanksDevice, enq_stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    prepareOp(c10d::OpType::ALLTOALL);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::ALLTOALL, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    memcpy((char*)dst + j * realSize,
                           (char*)inputTensors[j].data_ptr() + pos, realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    memcpy((char*)outputTensors[j].data_ptr() + pos,
                           (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensors[0].device().index());
        return worker_->putTaskCuda(
            c10d::OpType::ALLTOALL, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    cudaMemcpyAsync((char*)dst + j * realSize,
                                    (char*)inputTensors[j].data_ptr() + pos,
                                    realSize, cudaMemcpyDeviceToDevice,
                                    enq_stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    cudaMemcpyAsync((char*)outputTensors[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToDevice, enq_stream);
                }
            });
    }
}
c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions& opts) {
    prepareOp(c10d::OpType::BARRIER);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            // a non-zero tensorSize is required to ensure the worker task for
            // the barrier is created
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_,
            std::move(failed_ranks), [=](void*, size_t, size_t) {},
            [=](void*, size_t, size_t) {});
    } else {
        auto device_index = at::cuda::current_device();
        auto stream = at::cuda::getCurrentCUDAStream(device_index);
        return worker_->putTaskCuda(
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=](void*, size_t, size_t, const at::cuda::CUDAStream&) {},
            [=](void*, size_t, size_t, const at::cuda::CUDAStream&) {});
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::reduce(
    std::vector<at::Tensor>& tensors, const c10d::ReduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    prepareOp(c10d::OpType::REDUCE);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        auto numRanks = meta_->maxGroupSize;
        return worker_->putTaskCpu(
            c10d::OpType::REDUCE, tensorSize, root, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    memset((char*)tensor.data_ptr() + pos, 0, realSize);
                    launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                    opts.reduceOp, meta_->activeRanks);
                }
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::REDUCE, tensorSize, root, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, enq_stream);
            },
            [=, this](void* src, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                if (isRoot) {
                    cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                    enq_stream);
                    launchReduceKernel(tensor, pos, realSize, src,
                                       meta_->maxGroupSize, opts.reduceOp,
                                       meta_->activeRanksDevice, enq_stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::gather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::GatherOptions& opts) {
    int64_t root = opts.rootRank;
    bool isRoot = (root == rank_);
    TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    if (isRoot) {
        TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    }
    auto inputTensor = inputTensors.back();
    size_t tensorSize = inputTensor.numel() * inputTensor.element_size();
    prepareOp(c10d::OpType::GATHER);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::GATHER, tensorSize, root, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputTensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    auto outputTensors_ = outputTensors.back();
                    for (const auto j : c10::irange(outputTensors_.size())) {
                        memcpy((char*)outputTensors_[j].data_ptr() + pos,
                               (char*)src + j * realSize, realSize);
                    }
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputTensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::GATHER, tensorSize, root, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, enq_stream);
            },
            [=](void* src, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                if (isRoot) {
                    auto outputTensors_ = outputTensors.back();
                    for (const auto j : c10::irange(outputTensors_.size())) {
                        cudaMemcpyAsync(
                            (char*)outputTensors_[j].data_ptr() + pos,
                            (char*)src + j * realSize, realSize,
                            cudaMemcpyDeviceToDevice, enq_stream);
                    }
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::scatter(
    std::vector<at::Tensor>& outputTensors,
    std::vector<std::vector<at::Tensor>>& inputTensors,
    const c10d::ScatterOptions& opts) {
    int64_t root = opts.rootRank;
    bool isRoot = (root == rank_);
    if (isRoot) {
        TORCH_CHECK(inputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    }
    TORCH_CHECK(outputTensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto outputTensor = outputTensors.back();
    size_t tensorSize = outputTensor.numel() * outputTensor.element_size();
    prepareOp(c10d::OpType::SCATTER);
    auto failed_ranks = FailedRanksHint::allocate(meta_->maxGroupSize);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::SCATTER, tensorSize, root, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    auto inputTensors_ = inputTensors.back();
                    for (const auto j : c10::irange(inputTensors_.size())) {
                        memcpy((char*)dst + j * realSize,
                               (char*)inputTensors_[j].data_ptr() + pos,
                               realSize);
                    }
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                memcpy((char*)outputTensor.data_ptr() + pos, src, realSize);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(outputTensor.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::SCATTER, tensorSize, root, meta_, stream,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                if (isRoot) {
                    auto inputTensors_ = inputTensors.back();
                    for (const auto j : c10::irange(inputTensors_.size())) {
                        cudaMemcpyAsync(
                            (char*)dst + j * realSize,
                            (char*)inputTensors_[j].data_ptr() + pos, realSize,
                            cudaMemcpyDeviceToDevice, enq_stream);
                    }
                }
            },
            [=](void* src, size_t pos, size_t realSize,
                const at::cuda::CUDAStream& enq_stream) {
                cudaMemcpyAsync((char*)outputTensor.data_ptr() + pos, src,
                                realSize, cudaMemcpyDeviceToDevice, enq_stream);
            });
    }
}

void MooncakeBackend::shutdown() {
    if (isShutdown_) {
        return;
    }
    isShutdown_ = true;

    // If we encounter any hung operations, don't release resources
    // to avoid potential crash. Instead, we allow those resources to leak
    // and rely on the OS to reclaim them later.
    bool has_hung_operation = false;

    // Phase 1: Drain P2P tasks
    p2p_device_worker_->removeProxy(p2p_proxy_);
    has_hung_operation |= !p2p_proxy_->drainTasks();

    // Phase 2: Drain collective tasks for this backend
    has_hung_operation |= !worker_->drainTasks(meta_.get());

    // Phase 3: CUDA synchronization
    if (!isCpu_ && !has_hung_operation) {
        cudaDeviceSynchronize();
    }

    // Phase 4: Release resources
    if (has_hung_operation) {
        p2p_proxy_->abandonResources();
    }

    if (!has_hung_operation) {
        for (size_t i = 0; i < 2; i++) {
            ctx_.engine->unregisterLocalMemory(cpu_sync_send_region_[i]);
            ctx_.engine->unregisterLocalMemory(cpu_sync_recv_region_[i]);
            ctx_.engine->unregisterLocalMemory(send_buffer_[i]);
            ctx_.engine->unregisterLocalMemory(recv_buffer_[i]);
            delete[] cpu_sync_send_region_[i];
            delete[] cpu_sync_recv_region_[i];
            if (isCpu_) {
                free(send_buffer_[i]);
                free(recv_buffer_[i]);
            } else {
                cudaFree(send_buffer_[i]);
                cudaFree(recv_buffer_[i]);
            }
        }
        if (isCpu_) {
            delete[] meta_->activeRanks;
        } else {
            cudaFreeHost(meta_->activeRanks);
        }
        meta_->activeRanks = nullptr;
        meta_->activeRanksDevice = nullptr;
    }

    // Prevent zombie P2PProxy workers from dereferencing this backend after
    // destruction.  Must happen after drainTasks so in-flight failures can
    // still be reported during shutdown.
    if (meta_) {
        meta_->backend = nullptr;
    }
}

void MooncakeBackend::syncActiveRanksTensor() {
    // activeRanks is indexed by InGroupRank, length = max_group_size_.
    // The Python-visible tensor must also be max_group_size_ so that it stays
    // consistent with the shape produced by the constructor and so that the
    // indices align correctly with the in-group rank space.
    std::vector<int32_t> active_ranks(max_group_size_, 0);
    for (int i = 0; i < meta_->maxGroupSize; ++i) {
        active_ranks[i] = meta_->activeRanks[i] ? 1 : 0;
    }

    auto cpu_tensor = torch::tensor(active_ranks, torch::dtype(torch::kInt32));
    if (!meta_->activeRanksTensor.defined() ||
        meta_->activeRanksTensor.size(0) != max_group_size_) {
        meta_->activeRanksTensor = cpu_tensor;
        return;
    }
    meta_->activeRanksTensor.copy_(cpu_tensor);
}

void MooncakeBackend::extendGroupSizeTo(int newSize) {
    // Deprecated: in the Coordinator-based path, group size is determined
    // by GroupView.rank_order.  This is a no-op stub kept for compatibility.
    LOG(WARNING) << "MooncakeBackend::extendGroupSizeTo is deprecated; "
                 << "group size is now controlled by Coordinator's GroupView.";
}

int MooncakeBackend::getNumSyncedRanks() {
    if (!meta_) return 0;
    int count = 0;
    for (int i = 0; i < meta_->maxGroupSize; ++i) {
        if (agent_.maybeActivatable(meta_->group_id, i)) ++count;
    }
    return count;
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    std::vector<bool> output;
    output.reserve(ranks.size());
    for (const int rank : ranks) {
        output.push_back(agent_.maybeActivatable(meta_->group_id, rank));
    }
    return output;
}

void MooncakeBackend::recoverRanks(const std::vector<int>& ranks) {
    activateRanks(ranks);
}

ProposeViewUpdateResponse MooncakeBackend::activateRanks(
    const std::vector<int>& ranks) {
    // ranks are in-group (local) ranks; map to global ranks via rank_order.
    std::vector<GlobalRank> global_ranks;
    global_ranks.reserve(ranks.size());
    for (int r : ranks) {
        global_ranks.push_back(meta_->rank_order[r]);
    }
    auto resp = agent_.proposeActivate(meta_->group_id, global_ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        LOG(WARNING) << "MooncakeBackend: activate_rank rejected: "
                     << resp.reject_reason;
    }
    return resp;
}

ProposeViewUpdateResponse MooncakeBackend::deactivateRanks(
    const std::vector<int>& ranks) {
    // ranks are in-group (local) ranks; map to global ranks via rank_order.
    std::vector<GlobalRank> global_ranks;
    global_ranks.reserve(ranks.size());
    for (int r : ranks) {
        global_ranks.push_back(meta_->rank_order[r]);
    }
    auto resp = agent_.proposeDeactivate(meta_->group_id, global_ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        LOG(WARNING) << "MooncakeBackend: deactivate_rank rejected: "
                     << resp.reject_reason;
    }
    return resp;
}

void MooncakeBackend::joinGroup() {
    // Block until the Coordinator activates this rank in the group.
    agent_.waitUntilRankActive(meta_->group_id, meta_->globalRank,
                               std::chrono::seconds(30));
    LOG(INFO) << "joinGroup rank=" << meta_->globalRank
              << " group=" << meta_->group_id << " activated";
}

SyncAfterFailureResponse MooncakeBackend::syncAfterFailure() {
    return agent_.syncAfterFailure(meta_->group_id);
}

void MooncakeBackend::applyViewUpdate(const GroupView& view) {
    if (!meta_) return;

    // Ignore stale views that arrive out of order
    auto current_epoch = meta_->epoch.load(std::memory_order_acquire);
    if (view.epoch < current_epoch) {
        return;
    }

    // Membership boundary (activation/deactivation): reset the
    // collective sequence so that all ranks, including joiners and
    // replacements, agree on the double-buffer parity.
    bool epoch_changed = false;
    if (meta_->epoch.load(std::memory_order_acquire) != view.epoch) {
        meta_->taskCount = 0;
        epoch_changed = true;
    }

    // Rank order
    for (size_t local_rank = 0; local_rank < view.rank_order.size();
         ++local_rank) {
        TORCH_CHECK(static_cast<int32_t>(local_rank) < meta_->maxGroupSize,
                    "Bad group view");

        // rank order
        meta_->rank_order[local_rank] = view.rank_order[local_rank];
        const auto global_rank = view.rank_order[local_rank];

        // active ranks
        const auto& member = view.members[global_rank];
        meta_->activeRanks[local_rank] = member.isActive();
        if (member.endpoint.has_value()) {
            meta_->segmentInfos[local_rank] = *member.endpoint;
        }
    }

    // Keep the Python-visible activeRanksTensor in sync with the view.
    // FIXME: potential deadlock?
    syncActiveRanksTensor();

    // Publish epoch AFTER all data-plane state (activeRanks, segmentInfos,
    // etc.) is updated.  This ensures that a thread observing the new epoch
    // via getCurrentEpoch() (acquire) sees the complete membership state.
    if (epoch_changed) {
        meta_->epoch.store(view.epoch, std::memory_order_release);
    }
}

void MooncakeBackend::onPeerLinkReset(InGroupRank peer) {
    if (isShutdown_) return;
    if (p2p_proxy_) {
        p2p_proxy_->resetPeerState(peer);
    }
    if (peer >= 0 && peer < meta_->maxGroupSize) {
        meta_->segmentIDs[peer] = static_cast<TransferMetadata::SegmentID>(-1);
    }
}

void MooncakeBackend::refreshSegmentID(InGroupRank local) {
    auto global = meta_->rank_order[local];
    auto handle = ctx_.link_manager.resolvePeer(global);
    meta_->segmentIDs[local] =
        handle.has_value() ? *handle
                           : static_cast<TransferMetadata::SegmentID>(-1);
}

GroupEndpointPublication MooncakeBackend::buildEndpointMetadata() const {
    GroupEndpointPublication ep;
    ep.group_id = meta_->group_id;
    ep.endpoint_info = meta_->segmentInfos[meta_->rank];
    return ep;
}

}  // namespace mooncake
