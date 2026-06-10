#include <ATen/cuda/CUDAContext.h>
#include <cuda_alike.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <p2p_proxy.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <memory>
#include "memory_location.h"
#include "mooncake_worker.cuh"
#include "pg_utils.h"
#include "control_plane/link_manager.h"

namespace mooncake {

constexpr const char* REGISTER_BUFFER_ERROR_MSG =
    "Failed to register local memory.";
constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SYNC_OP_ERROR_MSG = "Expecting async op but got sync op.";
constexpr const char* REDUCE_OP_ERROR_MSG = "Only support SUM.";
constexpr const char* SPARSE_ERROR_MSG = "Sparse op not supported.";
constexpr const char* REDUCE_DTYPE_ERROR_MSG = "Unsupported reduce dtype: ";
constexpr int kBarrierDummyTensorSize = 1;

// All process-level state is in MooncakeProcessContext (pg_py.cpp).

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
    const int max_group_size = (options_ && options_->maxWorldSize_ > 0)
                                   ? options_->maxWorldSize_
                                   : size;

    TORCH_CHECK(max_group_size >= 0 &&
                    static_cast<size_t>(max_group_size) <= kMaxNumRanks,
                "max_world_size out of range");
    TORCH_CHECK(max_group_size >= size,
                "max_world_size must be >= process group size");
    TORCH_CHECK(rank >= 0 && rank < max_group_size, "rank out of valid range");

    max_group_size_ = max_group_size;
    const auto& globalRanks = distBackendOpts.global_ranks_in_group;

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

    // Engine and LinkManager are initialized by initControlPlane() (called
    // before the first MooncakeBackend constructor).  The external_engine
    // override still applies here.
    if (ctx_.external_engine) {
        ctx_.engine = ctx_.external_engine;
        ctx_.engine_initialized = true;
    }
    localServerName_ = ctx_.engine->getLocalIpAndPort();

    // Build rank_order from global_ranks_in_group (or identity fallback).
    // This is used for the GroupDeclaration and for meta_->rank_order.
    std::vector<GlobalRank> initial_rank_order;
    initial_rank_order.reserve(size);
    if (globalRanks.size() == static_cast<size_t>(size)) {
        for (int i = 0; i < size; ++i) {
            initial_rank_order.push_back(
                static_cast<GlobalRank>(globalRanks[i]));
        }
    } else {
        for (int i = 0; i < size; ++i) {
            initial_rank_order.push_back(static_cast<GlobalRank>(i));
        }
    }
    // Fill remaining slots for future joiners.
    for (int i = size; i < max_group_size_; ++i) {
        initial_rank_order.push_back(static_cast<GlobalRank>(i));
    }

    {
        std::string gr;
        for (size_t i = 0; i < globalRanks.size(); ++i) {
            if (i) gr += ",";
            gr += std::to_string(globalRanks[i]);
        }
        std::string ro;
        for (size_t i = 0; i < initial_rank_order.size(); ++i) {
            if (i) ro += ",";
            ro += std::to_string(initial_rank_order[i]);
        }
        LOG(INFO) << "MooncakeBackend: rank=" << rank << " size=" << size
                  << " globalRanks=[" << gr << "]"
                  << " initial_rank_order=[" << ro << "]"
                  << " globalRank=" << initial_rank_order[rank];
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
                         // Use max_group_size_ (capacity) rather than the
                         // initial world size so that P2PProxy can handle
                         // future peers during elastic extension/recovery.
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
    for (int i = 0; i < max_group_size_; ++i) {
        meta_->rank_order[i] = initial_rank_order[i];
    }
    meta_->size = max_group_size_;  // slot capacity
    meta_->activeSize = size;
    meta_->taskCount = 0;
    meta_->collectiveTimeoutUs = &ctx_.collective_timeout_us;
    meta_->engine = ctx_.engine;
    meta_->backend = this;
    p2p_proxy_->bindMeta(meta_);

    // Legacy activeRanks allocation (for backward compat with GPU kernel).
    if (isCpu) {
        meta_->activeRanks = new bool[kMaxNumRanks]{};
    } else {
        cudaHostAlloc(&meta_->activeRanks, kMaxNumRanks * sizeof(bool),
                      cudaHostAllocMapped);
        cudaHostGetDevicePointer(&meta_->activeRanksDevice, meta_->activeRanks,
                                 0);
    }
    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        meta_->activeRanks[i] = (i < static_cast<size_t>(size));
    }
    meta_->activeRanksTensor = at::ones(
        {max_group_size_},
        torch::dtype(torch::kInt32).device(isCpu ? torch::kCPU : torch::kCUDA));
    if (max_group_size_ != size) {
        meta_->activeRanksTensor.slice(0, size, max_group_size_).fill_(0);
    }

    // Store local endpoint info in segmentInfos for worker thread access.
    auto& local_ep = meta_->segmentInfos[meta_->globalRank];
    local_ep.send_buffer[0] = (uint64_t)send_buffer_[0];
    local_ep.send_buffer[1] = (uint64_t)send_buffer_[1];
    local_ep.recv_buffer[0] = (uint64_t)recv_buffer_[0];
    local_ep.recv_buffer[1] = (uint64_t)recv_buffer_[1];
    local_ep.send_sync[0] = (uint64_t)cpu_sync_send_region_[0];
    local_ep.send_sync[1] = (uint64_t)cpu_sync_send_region_[1];
    local_ep.recv_sync[0] = (uint64_t)cpu_sync_recv_region_[0];
    local_ep.recv_sync[1] = (uint64_t)cpu_sync_recv_region_[1];
    local_ep.p2p_credit_region = (uint64_t)p2p_proxy_->credit_region();
    local_ep.p2p_ack_region = (uint64_t)p2p_proxy_->ack_region();

    // ---- Agent-based bootstrap (new path) ----
    // Register this group with the Agent, publish local endpoint, and block
    // until the Coordinator returns the authoritative GroupView.
    {
        meta_->group_id = static_cast<int32_t>(ctx_.next_group_id);

        // Build GroupDeclaration.
        GroupDeclaration declaration;
        declaration.descriptor.group_id = meta_->group_id;
        declaration.descriptor.rank_order = std::move(initial_rank_order);
        declaration.auto_deactivate =
            options_ ? options_->autoDeactivateOnFailure_ : true;

        // Build initial_view: founding members are active.
        // Other ranks' endpoints arrive via publishEndpoint.
        declaration.initial_view.group_id = meta_->group_id;
        declaration.initial_view.epoch =
            0;  // Coordinator sets real epoch on activation
        declaration.initial_view.members.resize(max_group_size_);
        for (int i = 0; i < size; ++i) {
            declaration.initial_view
                .members[declaration.descriptor.rank_order[i]]
                .active = true;
        }
        declaration.initial_view.members[meta_->globalRank].endpoint_info =
            local_ep;
        declaration.initial_view.members[meta_->globalRank].endpoint_epoch =
            kInitialEndpointEpoch;

        // Bootstrap: wait for Agent registration before declaring the group.
        // The Coordinator rejects declareGroup if the agent is still OFFLINE.
        if (!agent_.waitUntilRegistered(std::chrono::seconds(30))) {
            throw std::runtime_error(
                "MooncakeBackend: Agent registration timed out");
        }

        // Declare group to Coordinator (synchronous — blocks until the
        // Coordinator has processed declareGroup).
        agent_.registerGroup(declaration, this);

        // Publish local endpoint (synchronous — group exists on Coordinator
        // because registerGroup returned).
        agent_.publishLocalEndpoint(GroupEndpointPublication{
            .group_id = meta_->group_id,
            .endpoint_epoch = kInitialEndpointEpoch,
            .endpoint_info = local_ep,
        });
        auto view = agent_.waitUntilGroupReady(meta_->group_id,
                                               std::chrono::seconds(30));
        applyViewChange(declaration.descriptor, view);
    }
    // ---- End Agent-based bootstrap ----

    // Register a lightweight Backend shim so that PyTorch's P2P dispatch path
    // (batch_isend_irecv → _get_backend → getBackend) can find a registered
    // Backend for this ProcessGroup.  The shim delegates send/recv back to us.
    auto deviceType = isCpu ? c10::DeviceType::CPU : c10::DeviceType::CUDA;
    auto shim = c10::make_intrusive<MooncakeP2PShim>(this);
    setBackend(deviceType, BackendType::CUSTOM, shim);
#ifndef MOONCAKE_EP_USE_MUSA
    setDefaultBackend(BackendType::CUSTOM);
#endif

    // Increment backend index
    ++ctx_.next_group_id;
}

MooncakeBackend::~MooncakeBackend() { shutdown(); }

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

c10::intrusive_ptr<c10d::Work> MooncakeBackend::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(dstRank >= 0 && dstRank < meta_->size,
                "P2P send: dstRank out of range.");

    auto contiguous = tensor.contiguous();
    auto status = std::make_shared<std::atomic<P2PProxy::OpStatus>>(
        P2PProxy::OpStatus::kPending);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(contiguous.device().index());
        stream = current_stream.stream();
    }

    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);

    TORCH_CHECK(p2p_proxy_, "P2P send proxy is not initialized.");
    p2p_proxy_->enqueueSend(P2PProxy::SendOp{
        .tensor_ = std::move(contiguous),
        .peer_rank_ = dstRank,
        .cuda_stream_ = stream,
        .status_ = status,
        .failed_ranks_ = failed_ranks.data(),
    });

    return c10::make_intrusive<MooncakeP2PWork>(status,
                                                std::move(failed_ranks));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(srcRank >= 0 && srcRank < meta_->size,
                "P2P recv: srcRank out of range.");

    auto target = tensor.is_contiguous() ? tensor : tensor.contiguous();
    auto status = std::make_shared<std::atomic<P2PProxy::OpStatus>>(
        P2PProxy::OpStatus::kPending);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(target.device().index());
        stream = current_stream.stream();
    }

    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);

    TORCH_CHECK(p2p_proxy_, "P2P recv proxy is not initialized.");
    p2p_proxy_->enqueueRecv(P2PProxy::RecvOp{
        .tensor_ = target,
        .original_tensor_ = tensor,
        .peer_rank_ = srcRank,
        .cuda_stream_ = stream,
        .status_ = status,
        .failed_ranks_ = failed_ranks.data(),
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
    if (isCpu_) {
        auto numRanks = meta_->size;
        auto* failed_ranks_ptr = failed_ranks.data();
        return worker_->putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                memset((char*)tensor.data_ptr() + pos, 0, realSize);
                launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_->activeRanks,
                                failed_ranks_ptr);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto* failed_ranks_dev_ptr = failed_ranks.dev_ptr;
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
                launchReduceKernel(tensor, pos, realSize, src, meta_->size,
                                   opts.reduceOp, meta_->activeRanksDevice,
                                   failed_ranks_dev_ptr, enq_stream);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputBuffer.data_ptr() + pos, realSize);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                for (int j = 0; j < meta_->size; ++j) {
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
                for (int j = 0; j < meta_->size; ++j) {
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
    if (isCpu_) {
        auto numRanks = meta_->size;
        auto* failed_ranks_ptr = failed_ranks.data();
        return worker_->putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            std::move(failed_ranks),
            [=, this](void* dst, size_t pos, size_t realSize) {
                for (int j = 0; j < meta_->size; ++j) {
                    if (!meta_->activeRanks[j]) continue;
                    memcpy((char*)dst + j * realSize,
                           (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                           realSize);
                }
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                memset((char*)outputBuffer.data_ptr() + pos, 0, realSize);
                launchReduceCpu(outputBuffer, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_->activeRanks,
                                failed_ranks_ptr);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        auto* failed_ranks_dev_ptr = failed_ranks.dev_ptr;
        return worker_->putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_, stream,
            std::move(failed_ranks),
            [=, this](void* dst, size_t pos, size_t realSize,
                      const at::cuda::CUDAStream& enq_stream) {
                for (int j = 0; j < meta_->size; ++j) {
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
                                   meta_->size, opts.reduceOp,
                                   meta_->activeRanksDevice,
                                   failed_ranks_dev_ptr, enq_stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
    if (isCpu_) {
        auto numRanks = meta_->size;
        auto* failed_ranks_ptr = failed_ranks.data();
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
                                    opts.reduceOp, meta_->activeRanks,
                                    failed_ranks_ptr);
                }
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto* failed_ranks_dev_ptr = failed_ranks.dev_ptr;
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
                    launchReduceKernel(tensor, pos, realSize, src, meta_->size,
                                       opts.reduceOp, meta_->activeRanksDevice,
                                       failed_ranks_dev_ptr, enq_stream);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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
    auto failed_ranks = FailedRanks::allocate(meta_->size, isCpu_);
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

    // Phase 3: Drain warm-up transfers for connection poller (legacy)
    // ConnectionPoller cleanup: no-op — connection_ctx_ is not created
    // in the Agent-based path.

    // Phase 4: CUDA synchronization
    if (!isCpu_ && !has_hung_operation) {
        cudaDeviceSynchronize();
    }

    // Phase 5: Release resources if no hung operations
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
}

void MooncakeBackend::syncActiveRanksTensor() {
    // Read from meta_->activeRanks (synced by control plane via
    // applyViewChange).
    std::vector<int32_t> active_ranks(meta_->size);
    for (int i = 0; i < meta_->size; ++i) {
        active_ranks[i] = (meta_->activeRanks && meta_->activeRanks[i]) ? 1 : 0;
    }

    auto cpu_tensor = torch::tensor(active_ranks, torch::dtype(torch::kInt32));
    if (!meta_->activeRanksTensor.defined() ||
        meta_->activeRanksTensor.size(0) != meta_->size) {
        meta_->activeRanksTensor =
            cpu_tensor.to(isCpu_ ? torch::kCPU : torch::kCUDA);
        return;
    }

    if (meta_->activeRanksTensor.device().is_cpu()) {
        meta_->activeRanksTensor.copy_(cpu_tensor);
    } else {
        meta_->activeRanksTensor.copy_(
            cpu_tensor.to(meta_->activeRanksTensor.device()));
    }
}

void MooncakeBackend::extendGroupSizeTo(int newSize) {
    // Deprecated: in the Coordinator-based path, group size is determined
    // by GroupView.rank_order.  This is a no-op stub kept for compatibility.
    LOG(WARNING) << "MooncakeBackend::extendGroupSizeTo is deprecated; "
                 << "group size is now controlled by Coordinator's GroupView.";
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    std::vector<bool> output;
    output.reserve(ranks.size());
    for (const int rank : ranks) {
        TORCH_CHECK(rank >= 0 && rank < kMaxNumRanks, "Rank out of range");
        output.push_back(
            ctx_.link_manager.isRankReady(static_cast<GlobalRank>(rank)));
    }
    return output;
}

void MooncakeBackend::recoverRanks(const std::vector<int>& ranks) {
    // Deprecated: replaced by Agent::proposeActivate.  This is a no-op
    // stub kept for API compatibility — it now just calls activateRank.
    LOG(WARNING) << "MooncakeBackend::recoverRanks is deprecated; "
                 << "calling activateRank instead.";
    activateRank(ranks);
}

ProposeViewUpdateResponse MooncakeBackend::activateRank(
    const std::vector<int>& ranks) {
    std::vector<GlobalRank> global_ranks;
    global_ranks.reserve(ranks.size());
    for (int r : ranks) {
        global_ranks.push_back(static_cast<GlobalRank>(r));
    }
    auto resp = agent_.proposeActivate(meta_->group_id, global_ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        LOG(ERROR) << "MooncakeBackend: activate_rank rejected: "
                   << resp.reject_reason;
    }
    return resp;
}

ProposeViewUpdateResponse MooncakeBackend::deactivateRank(
    const std::vector<int>& ranks) {
    std::vector<GlobalRank> global_ranks;
    global_ranks.reserve(ranks.size());
    for (int r : ranks) {
        global_ranks.push_back(static_cast<GlobalRank>(r));
    }
    auto resp = agent_.proposeDeactivate(meta_->group_id, global_ranks);
    if (resp.status == ViewUpdateStatus::Rejected) {
        LOG(ERROR) << "MooncakeBackend: deactivate_rank rejected: "
                   << resp.reject_reason;
    }
    return resp;
}

void MooncakeBackend::joinGroup() {
    // Deprecated: in the Coordinator-based path, the Agent handles all
    // registration and group-join logic at construction time.  This is
    // a no-op stub kept for API compatibility.
    LOG(WARNING) << "MooncakeBackend::joinGroup is deprecated; "
                 << "groups are auto-registered by Agent during construction.";
}

// =========================================================================
// New methods for Agent/Host integration
// =========================================================================

void MooncakeBackend::applyViewChange(const GroupDescriptor& descriptor,
                                      const GroupView& view) {
    // Sync rank_order for P2P proxy InGroupRank -> GlobalRank mapping.
    if (meta_) {
        if (meta_->epoch != view.epoch) {
            // Membership boundary (extension/recovery/deactivation): reset the
            // collective sequence so that all ranks, including joiners and
            // replacements, agree on the double-buffer parity. Re-applying the
            // same view (e.g. on LinkUp) leaves epoch unchanged, so in-flight
            // double buffering is not disrupted.
            meta_->taskCount = 0;
            meta_->epoch = view.epoch;
        }
        for (size_t i = 0; i < descriptor.rank_order.size() && i < kMaxNumRanks;
             ++i) {
            meta_->rank_order[i] = descriptor.rank_order[i];
        }
    }

    // Sync TransferGroupMeta for data plane access (worker thread, P2P proxy).
    // The Coordinator guarantees all active members have published endpoints
    // and are HEALTHY before transitioning to Ready status, but the local TE
    // link may still be establishing.  If resolvePeer() misses a peer, the
    // LinkUp handler in AgentStateMachine will re-apply the Ready view to
    // refresh the cached segment ID once the link is up.
    if (meta_ && meta_->activeRanks) {
        for (int i = 0;
             i < static_cast<int>(view.members.size()) && i < meta_->size;
             ++i) {
            meta_->activeRanks[i] = view.members[i].active;
            if (view.members[i].active &&
                view.members[i].endpoint_epoch != kInvalidEpoch) {
                meta_->segmentInfos[i] = view.members[i].endpoint_info;
                auto handle =
                    ctx_.link_manager.resolvePeer(static_cast<GlobalRank>(i));
                if (handle) {
                    meta_->segmentIDs[i] = handle->target_id;
                } else {
                    LOG(WARNING)
                        << "MooncakeBackend: applyViewChange rank="
                        << meta_->globalRank << " TE link to peer=" << i
                        << " not ready yet; segmentID will be refreshed on "
                           "LinkUp";
                }
            }
        }
    }

    // activeRanksTensor sync happens inside GPU kernels (launchEnqueueTask-
    // Kernel) and the CPU path (putTaskCpu).  Do NOT sync from the executor
    // thread — a host→device copy while a kernel is running can deadlock.

    // Recalculate activeSize from the view (count of active members).
    int new_active_size = 0;
    for (const auto& member : view.members) {
        if (member.active) ++new_active_size;
    }
    meta_->activeSize = new_active_size;
}

void MooncakeBackend::onPeerLinkReset(GlobalRank peer) {
    if (p2p_proxy_) {
        p2p_proxy_->resetPeerState(peer);
    }
}

void MooncakeBackend::markViewStale() {
    // Mark all peers as inactive so worker threads stop using them.
    if (meta_ && meta_->activeRanks) {
        for (int i = 0; i < meta_->size; ++i) {
            meta_->activeRanks[i] = false;
        }
    }
}

GroupEndpointPublication MooncakeBackend::buildEndpointMetadata() const {
    GroupEndpointPublication ep;
    ep.group_id = meta_->group_id;
    ep.endpoint_epoch = kInitialEndpointEpoch;
    ep.endpoint_info = meta_->segmentInfos[meta_->globalRank];
    // agent_session_epoch is NOT set here — the Host fills it in the
    // enclosing PublishEndpointRequest before sending the RPC.
    return ep;
}

}  // namespace mooncake
