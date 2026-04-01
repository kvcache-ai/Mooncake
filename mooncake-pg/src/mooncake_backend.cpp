#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <cuda_alike.h>
#include <mooncake_backend.h>
#include <p2p_proxy.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <memory>
#include "connection_poller.h"
#include "memory_location.h"
#include "mooncake_worker.cuh"

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

std::string MooncakeBackend::hostIp_ = "127.0.0.1";
// leaky singleton to avoid destructor fiasco problem
TransferEngine* MooncakeBackend::engine_ = new TransferEngine(true);
// worker_ is now owned per backend instance via MooncakeWorkerManager.
bool MooncakeBackend::engineInitialized_ = false;
int MooncakeBackend::backendIndex_ = 0;

namespace {

std::vector<uint8_t> serializeActiveRanks(const bool* activeRanks, int size) {
    std::vector<uint8_t> bytes(size);
    for (int i = 0; i < size; ++i) {
        bytes[i] = activeRanks[i] ? 1 : 0;
    }
    return bytes;
}

void deserializeActiveRanks(const std::vector<uint8_t>& bytes,
                            bool* activeRanks, int size) {
    TORCH_CHECK(static_cast<int>(bytes.size()) == size,
                "Unexpected active-ranks snapshot size.");
    for (int i = 0; i < size; ++i) {
        activeRanks[i] = (bytes[i] != 0);
    }
}

}  // namespace

// Async Work implementation for P2P operations processed by worker threads.
class MooncakeP2PWork : public ::c10d::Work {
   public:
    explicit MooncakeP2PWork(std::shared_ptr<std::atomic<bool>> completed)
        : Work(-1, c10d::OpType::UNKNOWN), completed_(completed) {}

    bool isCompleted() override {
        return completed_->load(std::memory_order_acquire);
    }

    bool wait(std::chrono::milliseconds timeout) override {
        if (completed_->load(std::memory_order_acquire)) {
            return true;
        }

        auto start = std::chrono::steady_clock::now();
        while (!completed_->load(std::memory_order_acquire)) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(now -
                                                                      start);
            if (timeout.count() > 0 && elapsed >= timeout) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        return true;
    }

   private:
    std::shared_ptr<std::atomic<bool>> completed_;
};

/**
 * @brief Initialize Mooncake backend state from the PyTorch process-group
 * information and optional Mooncake-specific options.
 */
MooncakeBackend::MooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : Backend(distBackendOpts.group_rank, distBackendOpts.group_size),
      options_(std::move(options)),
      isCpu_(isCpu) {
    auto store = std::move(distBackendOpts.store);
    const int rank = distBackendOpts.group_rank;
    const int size = distBackendOpts.group_size;
    const auto& globalRanks = distBackendOpts.global_ranks_in_group;

    // Get device data
    std::string location;
    int deviceCount = 0;
    cudaError_t err = cudaGetDeviceCount(&deviceCount);
    if (err != cudaSuccess || deviceCount == 0) {
        location = kWildcardLocation;
    } else {
        int deviceId_;
        err = cudaGetDevice(&deviceId_);
        TORCH_CHECK(!err, c10::str("Failed to get device id"));
        location = GPU_PREFIX + std::to_string(deviceId_);
    }

    // Initialize transfer engine
    if (!engineInitialized_) {
        engine_->init(P2PHANDSHAKE, hostIp_);
        engineInitialized_ = true;
    }
    localServerName_ = engine_->getLocalIpAndPort();
    // construct local to global rank map
    if (globalRanks.size() == static_cast<size_t>(size)) {
        for (int i = 0; i < size; ++i) {
            local2global_rank_map_[i] = static_cast<uint64_t>(globalRanks[i]);
        }
    } else {
        for (int i = 0; i < size; ++i) {
            local2global_rank_map_[i] = i;
        }
    }

    // Register buffers
    if (isCpu) {
        for (size_t i = 0; i < 2; i++) {
            send_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(send_buffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            int rc = engine_->registerLocalMemory(send_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            recv_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(recv_buffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            int rc = engine_->registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = engine_->registerLocalMemory(send_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = engine_->registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    }

    // Register CPU sync regions
    TORCH_CHECK(static_cast<size_t>(size) <= kMaxNumRanks,
                "The number of ranks exceeds the limit.");
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks]{};
        int rc = engine_->registerLocalMemory(cpu_sync_send_region_[i],
                                              kMaxNumRanks * sizeof(int32_t),
                                              kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks]{};
        int rc = engine_->registerLocalMemory(cpu_sync_recv_region_[i],
                                              kMaxNumRanks * sizeof(int32_t),
                                              kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    auto& dev_worker_mgr = P2PDeviceWorkerManager::GetInstance();
    int cuda_device_index = isCpu_ ? -1 : at::cuda::current_device();

    if (isCpu_)
        p2p_device_worker_ = dev_worker_mgr.GetCPUWorker();
    else
        p2p_device_worker_ = dev_worker_mgr.GetCUDAWorker(cuda_device_index);

    auto& worker_mgr = MooncakeWorkerManager::GetInstance();
    if (isCpu_)
        worker_ = worker_mgr.GetCPUWorker();
    else
        worker_ = worker_mgr.GetCUDAWorker(cuda_device_index);
    if (!isCpu_) {
        preloadReduceKernels();
    }
    worker_->Start();

    p2p_proxy_ = std::make_shared<P2PProxy>(
        engine_, P2PProxy::Options{
                     .is_cpu = isCpu_,
                     .rank = rank_,
                     .size = size_,
                     .cuda_device_index = cuda_device_index,
                     .location = location,
                 });
    p2p_device_worker_->registerProxy(p2p_proxy_);

    meta_ = std::make_shared<TransferGroupMeta>();
    connection_ctx_ = std::make_shared<ConnectionContext>(
        backendIndex_, rank, size, options_ && options_->isExtension_,
        local2global_rank_map_, store, meta_, p2p_proxy_, engine_);

    rank_info.send_buffer[0] = (uint64_t)send_buffer_[0];
    rank_info.send_buffer[1] = (uint64_t)send_buffer_[1];
    rank_info.recv_buffer[0] = (uint64_t)recv_buffer_[0];
    rank_info.recv_buffer[1] = (uint64_t)recv_buffer_[1];
    rank_info.send_sync[0] = (uint64_t)cpu_sync_send_region_[0];
    rank_info.send_sync[1] = (uint64_t)cpu_sync_send_region_[1];
    rank_info.recv_sync[0] = (uint64_t)cpu_sync_recv_region_[0];
    rank_info.recv_sync[1] = (uint64_t)cpu_sync_recv_region_[1];
    rank_info.warmup_buffer[0] =
        (uint64_t)connection_ctx_->warmup_send_region();
    rank_info.warmup_buffer[1] =
        (uint64_t)connection_ctx_->warmup_recv_region();
    rank_info.p2p_send_buffer = (uint64_t)p2p_proxy_->send_buffer();
    rank_info.p2p_recv_buffer = (uint64_t)p2p_proxy_->recv_buffer();
    rank_info.p2p_ctrl_send = (uint64_t)p2p_proxy_->ctrl_send_region();
    rank_info.p2p_ctrl_recv = (uint64_t)p2p_proxy_->ctrl_recv_region();

    // Sync metadata
    std::vector<uint8_t> rank_info_bytes(sizeof(SegmentInfo));
    memcpy(rank_info_bytes.data(), &rank_info, sizeof(SegmentInfo));
    meta_->rank = rank;
    meta_->size = size;
    meta_->taskCount = 0;
    if (isCpu) {
        meta_->activeRanks = new bool[kMaxNumRanks];
    } else {
        cudaHostAlloc(&meta_->activeRanks, kMaxNumRanks * sizeof(bool),
                      cudaHostAllocMapped);
        cudaHostGetDevicePointer(&meta_->activeRanksDevice, meta_->activeRanks,
                                 0);
    }
    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        meta_->activeRanks[i] = true;
    }
    if (options_ && options_->activeRanks_.defined()) {
        TORCH_CHECK(options_->activeRanks_.dtype() == at::kInt,
                    "activeRanks must be int.");
        if (isCpu) {
            TORCH_CHECK(options_->activeRanks_.device().is_cpu(),
                        "activeRanks must be on CPU.");
        } else {
            TORCH_CHECK(options_->activeRanks_.device().is_cuda(),
                        "activeRanks must be on CUDA.");
        }
        meta_->activeRanksTensor = options_->activeRanks_;
    } else {
        meta_->activeRanksTensor =
            at::ones({size}, torch::dtype(torch::kInt32)
                                 .device(isCpu ? torch::kCPU : torch::kCUDA));
    }
    meta_->engine = engine_;
    meta_->store = store;
    meta_->backendIndex = backendIndex_;
    meta_->bufferBaseIndex = backendIndex_ * 10;
    p2p_proxy_->BindMeta(meta_);

    connection_ctx_->bootstrapLocalPeer(localServerName_, rank_info);
    if (options_ && options_->isExtension_) {
        setLocalOnlyActiveRanks();
    } else {
        publishLocalPeerMetadata();
        ConnectionPoller::GetInstance().registerContext(connection_ctx_);
        connectionPollerRegistered_ = true;
        connection_ctx_->waitUntilAllConnected();
    }

    // Increment backend index
    ++backendIndex_;
}

MooncakeBackend::~MooncakeBackend() { shutdown(); }

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

c10::intrusive_ptr<c10d::Work> MooncakeBackend::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    connection_ctx_->waitUntilNewRanksConnected();

    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_->store, "P2P send requires a valid Store.");
    TORCH_CHECK(dstRank >= 0 && dstRank < size_,
                "P2P send: dstRank out of range.");

    auto contiguous = tensor.contiguous();
    auto completed = std::make_shared<std::atomic<bool>>(false);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(contiguous.device().index());
        stream = current_stream.stream();
    }

    TORCH_CHECK(p2p_proxy_, "P2P send proxy is not initialized.");
    p2p_proxy_->EnqueueSend(P2PProxy::SendOp{
        .tensor_ = std::move(contiguous),
        .peer_rank_ = dstRank,
        .cuda_stream_ = stream,
        .completed_ = completed,
    });

    return c10::make_intrusive<MooncakeP2PWork>(completed);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    connection_ctx_->waitUntilNewRanksConnected();

    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_->store, "P2P recv requires a valid Store.");
    TORCH_CHECK(srcRank >= 0 && srcRank < size_,
                "P2P recv: srcRank out of range.");

    auto target = tensor.is_contiguous() ? tensor : tensor.contiguous();
    auto completed = std::make_shared<std::atomic<bool>>(false);
    cudaStream_t stream = nullptr;
    if (!isCpu_) {
        auto current_stream =
            at::cuda::getCurrentCUDAStream(target.device().index());
        stream = current_stream.stream();
    }

    TORCH_CHECK(p2p_proxy_, "P2P recv proxy is not initialized.");
    p2p_proxy_->EnqueueRecv(P2PProxy::RecvOp{
        .tensor_ = target,
        .original_tensor_ = tensor,
        .peer_rank_ = srcRank,
        .cuda_stream_ = stream,
        .completed_ = completed,
    });

    return c10::make_intrusive<MooncakeP2PWork>(completed);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::broadcast(
    std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::BROADCAST, tensorSize, root, meta_, connection_ctx_,
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
            c10d::OpType::BROADCAST, tensorSize, root, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos,
                                    realSize, cudaMemcpyDeviceToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemcpyAsync((char*)tensor.data_ptr() + pos, src, realSize,
                                cudaMemcpyDeviceToDevice, stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    TORCH_CHECK(opts.sparseIndices == std::nullopt, SPARSE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    if (isCpu_) {
        auto numRanks = meta_->size;
        return worker_->putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_, connection_ctx_,
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
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, stream);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                stream);
                launchReduceKernel(tensor, pos, realSize, src, meta_->size,
                                   opts.reduceOp, meta_->activeRanksDevice,
                                   stream);
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
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::ALLGATHER, tensorSize, 0, meta_, connection_ctx_,
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
            c10d::OpType::ALLGATHER, tensorSize, 0, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    cudaMemcpyAsync((char*)outputTensors_[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToDevice, stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions& opts) {
    size_t tensorSize = inputBuffer.numel() * inputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = meta_->size;
        return worker_->putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            connection_ctx_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)inputBuffer.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(numRanks)) {
                    memcpy(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize);
                }
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_->putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            connection_ctx_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputBuffer.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, stream);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_->size)) {
                    cudaMemcpyAsync(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize,
                        cudaMemcpyDeviceToDevice, stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    size_t tensorSize = outputBuffer.numel() * outputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = meta_->size;
        return worker_->putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            connection_ctx_,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(numRanks)) {
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
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            connection_ctx_, stream,
            [=, this](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_->size)) {
                    cudaMemcpyAsync(
                        (char*)dst + j * realSize,
                        (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                        realSize, cudaMemcpyDeviceToDevice, stream);
                }
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)outputBuffer.data_ptr() + pos, 0,
                                realSize, stream);
                launchReduceKernel(outputBuffer, pos, realSize, src,
                                   meta_->size, opts.reduceOp,
                                   meta_->activeRanksDevice, stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::ALLTOALL, tensorSize, 0, meta_, connection_ctx_,
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
            c10d::OpType::ALLTOALL, tensorSize, 0, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    cudaMemcpyAsync((char*)dst + j * realSize,
                                    (char*)inputTensors[j].data_ptr() + pos,
                                    realSize, cudaMemcpyDeviceToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    cudaMemcpyAsync((char*)outputTensors[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToDevice, stream);
                }
            });
    }
}
c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions& opts) {
    if (isCpu_) {
        return worker_->putTaskCpu(
            // a non-zero tensorSize is required to ensure the worker task for
            // the barrier is created
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_,
            connection_ctx_, [=](void*, size_t, size_t) {},
            [=](void*, size_t, size_t) {});
    } else {
        auto device_index = at::cuda::current_device();
        auto stream = c10::cuda::getDefaultCUDAStream(device_index);
        return worker_->putTaskCuda(
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_,
            connection_ctx_, stream, [=](void*, size_t, size_t) {},
            [=](void*, size_t, size_t) {});
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::reduce(
    std::vector<at::Tensor>& tensors, const c10d::ReduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    if (isCpu_) {
        auto numRanks = meta_->size;
        return worker_->putTaskCpu(
            c10d::OpType::REDUCE, tensorSize, root, meta_, connection_ctx_,
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
            c10d::OpType::REDUCE, tensorSize, root, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, stream);
            },
            [=, this](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                    stream);
                    launchReduceKernel(tensor, pos, realSize, src, meta_->size,
                                       opts.reduceOp, meta_->activeRanksDevice,
                                       stream);
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
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::GATHER, tensorSize, root, meta_, connection_ctx_,
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
            c10d::OpType::GATHER, tensorSize, root, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    auto outputTensors_ = outputTensors.back();
                    for (const auto j : c10::irange(outputTensors_.size())) {
                        cudaMemcpyAsync(
                            (char*)outputTensors_[j].data_ptr() + pos,
                            (char*)src + j * realSize, realSize,
                            cudaMemcpyDeviceToDevice, stream);
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
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::SCATTER, tensorSize, root, meta_, connection_ctx_,
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
            c10d::OpType::SCATTER, tensorSize, root, meta_, connection_ctx_,
            stream,
            [=](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    auto inputTensors_ = inputTensors.back();
                    for (const auto j : c10::irange(inputTensors_.size())) {
                        cudaMemcpyAsync(
                            (char*)dst + j * realSize,
                            (char*)inputTensors_[j].data_ptr() + pos, realSize,
                            cudaMemcpyDeviceToDevice, stream);
                    }
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemcpyAsync((char*)outputTensor.data_ptr() + pos, src,
                                realSize, cudaMemcpyDeviceToDevice, stream);
            });
    }
}

void MooncakeBackend::shutdown() {
    if (isShutdown_) {
        return;
    }
    isShutdown_ = true;

    p2p_device_worker_->removeProxy(p2p_proxy_);
    p2p_proxy_.reset();

    connection_ctx_->shutdown();
    if (connectionPollerRegistered_) {
        ConnectionPoller::GetInstance().removeContext(connection_ctx_);
        connectionPollerRegistered_ = false;
    }

    for (size_t i = 0; i < 2; i++) {
        engine_->unregisterLocalMemory(cpu_sync_send_region_[i]);
        engine_->unregisterLocalMemory(cpu_sync_recv_region_[i]);
        engine_->unregisterLocalMemory(send_buffer_[i]);
        engine_->unregisterLocalMemory(recv_buffer_[i]);
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
}

void MooncakeBackend::syncActiveRanksTensor() {
    std::vector<int32_t> active_ranks(meta_->size);
    for (int i = 0; i < meta_->size; ++i) {
        active_ranks[i] = meta_->activeRanks[i] ? 1 : 0;
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

void MooncakeBackend::publishLocalPeerMetadata() {
    TORCH_CHECK(meta_->store,
                "Publishing local peer metadata requires a valid Store.");

    std::vector<uint8_t> rank_info_bytes(sizeof(SegmentInfo));
    memcpy(rank_info_bytes.data(), &rank_info, sizeof(SegmentInfo));

    auto bufferKey =
        ConnectionContext::getBufferStoreKey(meta_->backendIndex, rank_);
    meta_->store->set(bufferKey, rank_info_bytes);

    auto serverNameKey =
        ConnectionContext::getServerNameStoreKey(meta_->backendIndex, rank_);
    meta_->store->set(serverNameKey, localServerName_);
}

void MooncakeBackend::setLocalOnlyActiveRanks() {
    for (int i = 0; i < meta_->size; ++i) {
        meta_->activeRanks[i] = (i == meta_->rank);
    }
    syncActiveRanksTensor();
}

void MooncakeBackend::waitForExtensionState() {
    TORCH_CHECK(meta_->store, "Recovery join requires a valid Store.");

    auto task_count_key = ConnectionContext::getExtensionTaskCountStoreKey(
        meta_->backendIndex, rank_);
    auto active_ranks_key = ConnectionContext::getExtensionActiveRanksStoreKey(
        meta_->backendIndex, rank_);

    while (true) {
        if (meta_->store->check({task_count_key, active_ranks_key})) {
            auto task_count_data = meta_->store->get(task_count_key);
            std::string task_count(task_count_data.begin(),
                                   task_count_data.end());
            meta_->taskCount = std::stoi(task_count);

            auto active_ranks = meta_->store->get(active_ranks_key);
            deserializeActiveRanks(active_ranks, meta_->activeRanks,
                                   meta_->size);
            syncActiveRanksTensor();
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

int MooncakeBackend::getNumSyncedRanks() {
    std::vector<at::Tensor> tensors;
    tensors.emplace_back(torch::tensor(
        connection_ctx_->getTotalConnectedPeers(),
        torch::dtype(torch::kInt).device(isCpu_ ? torch::kCPU : torch::kCUDA)));
    c10d::AllreduceOptions opts{
        .reduceOp = c10d::ReduceOp::MIN,
    };
    auto work = allreduce(tensors, opts);
    work->wait();
    if (!isCpu_) {
        auto stream =
            at::cuda::getCurrentCUDAStream(tensors[0].device().index());
        cudaStreamSynchronize(stream);
    }
    return tensors[0].cpu().item<int>();
}

void MooncakeBackend::extendGroupSizeTo(int newSize) {
    const int oldSize = meta_->size;
    if (newSize == oldSize) return;

    TORCH_CHECK(newSize >= 0 && static_cast<size_t>(newSize) < kMaxNumRanks,
                "Size out of range");
    TORCH_CHECK(newSize >= oldSize, "newSize < oldSize");

    LOG(INFO) << "Backend " << backendIndex_ << " rank " << rank_
              << ": Group size extend to " << newSize;

    meta_->size = newSize;
    meta_->taskCount = 0;

    // Initialize new rank's metadata
    for (int i = oldSize; i < newSize; ++i) {
        local2global_rank_map_[i] = i;
        meta_->activeRanks[i] = true;
    }

    auto& tensor = meta_->activeRanksTensor;
    tensor.resize_({newSize});
    tensor.slice(0, oldSize, newSize).fill_(1);

    connection_ctx_->extendGroupSizeTo(newSize);
    // After extendGroupSizeTo, we don't `waitUntilNewRanksConnected` here
    // but do it in the first task. This enables client code to overlap
    // execution between `extendGroupSizeTo` and the first communication call.
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    bool activeRanksBackup[kMaxNumRanks];
    while (true) {
        std::vector<int> input;
        for (const int rank : ranks) {
            TORCH_CHECK(rank >= 0 && static_cast<size_t>(rank) < kMaxNumRanks,
                        "Rank out of range");
            input.push_back(meta_->peerConnected[rank]);
        }
        for (int i = 0; i < meta_->size; i++) {
            activeRanksBackup[i] = meta_->activeRanks[i];
        }

        std::vector<at::Tensor> tensors;
        tensors.emplace_back(torch::tensor(
            input, torch::dtype(torch::kInt)
                       .device(isCpu_ ? torch::kCPU : torch::kCUDA)));
        c10d::AllreduceOptions opts{
            .reduceOp = c10d::ReduceOp::MIN,
        };
        auto work = allreduce(tensors, opts);
        work->wait();
        if (!isCpu_) {
            auto stream =
                at::cuda::getCurrentCUDAStream(tensors[0].device().index());
            cudaStreamSynchronize(stream);
        }
        bool activeRanksChanged = false;
        for (int i = 0; i < meta_->size; i++) {
            if (activeRanksBackup[i] != meta_->activeRanks[i]) {
                activeRanksChanged = true;
                break;
            }
        }

        if (!activeRanksChanged) {
            std::vector<bool> output;
            for (int i = 0; i < tensors[0].size(0); ++i) {
                output.push_back(tensors[0].cpu()[i].item<int>() != 0);
            }
            return output;
        }
    }
}

void MooncakeBackend::recoverRanks(const std::vector<int>& ranks) {
    TORCH_CHECK(meta_->store, "Rank recovery requires a valid Store.");

    for (const int rank : ranks) {
        TORCH_CHECK(rank >= 0 && static_cast<size_t>(rank) < kMaxNumRanks,
                    "Rank out of range");
        TORCH_CHECK(meta_->peerConnected[rank]);
        meta_->activeRanks[rank] = true;
    }

    syncActiveRanksTensor();
    auto active_ranks_snapshot =
        serializeActiveRanks(meta_->activeRanks, meta_->size);
    for (const int rank : ranks) {
        meta_->store->set(ConnectionContext::getExtensionTaskCountStoreKey(
                              meta_->backendIndex, rank),
                          std::to_string(meta_->taskCount));
        meta_->store->set(ConnectionContext::getExtensionActiveRanksStoreKey(
                              meta_->backendIndex, rank),
                          active_ranks_snapshot);
    }
}

void MooncakeBackend::joinGroup() {
    TORCH_CHECK(options_ && options_->isExtension_,
                "joinGroup is only valid for extension backends.");
    connection_ctx_->setDummy(false);
    publishLocalPeerMetadata();
    if (!connectionPollerRegistered_) {
        ConnectionPoller::GetInstance().registerContext(connection_ctx_);
        connectionPollerRegistered_ = true;
    }
    connection_ctx_->waitUntilAllConnected();
    waitForExtensionState();
}
}  // namespace mooncake
