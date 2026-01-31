#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <memory>

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
TransferEngine MooncakeBackend::engine_ = TransferEngine(true);
bool MooncakeBackend::engineInitialized_ = false;
int MooncakeBackend::backendIndex_ = 0;
MooncakeWorker MooncakeBackend::worker_;

namespace {

// Async Work implementation for P2P operations processed by worker threads.
class MooncakeP2PWork : public ::c10d::Work {
   public:
    MooncakeP2PWork(std::shared_ptr<std::atomic<bool>> completed,
                    std::shared_ptr<std::string> errorMsg)
        : Work(-1, c10d::OpType::UNKNOWN),
          completed_(completed),
          errorMsg_(errorMsg) {}

    bool isCompleted() override {
        return completed_->load(std::memory_order_acquire);
    }

    bool wait(std::chrono::milliseconds timeout) override {
        if (completed_->load(std::memory_order_acquire)) {
            if (errorMsg_ && !errorMsg_->empty()) {
                TORCH_CHECK(false, "P2P operation failed: ", *errorMsg_);
            }
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

        if (errorMsg_ && !errorMsg_->empty()) {
            TORCH_CHECK(false, "P2P operation failed: ", *errorMsg_);
        }
        return true;
    }

   private:
    std::shared_ptr<std::atomic<bool>> completed_;
    std::shared_ptr<std::string> errorMsg_;
};

}  // namespace

MooncakeBackend::MooncakeBackend(
    c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : Backend(rank, size), isCpu_(isCpu) {
    // Initialize transfer engine
    if (!engineInitialized_) {
        engine_.init(P2PHANDSHAKE, hostIp_);
        engineInitialized_ = true;
    }
    auto localRpcMeta = engine_.getMetadata()->localRpcMeta();
    std::string localServerName = localRpcMeta.ip_or_host_name + ":" +
                                  std::to_string(localRpcMeta.rpc_port);

    // Register buffers
    if (isCpu) {
        for (size_t i = 0; i < 2; i++) {
            send_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(send_buffer_[i],
                        c10::str("Failed to allocate CPU send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            recv_buffer_[i] = malloc(kBufferSize);
            TORCH_CHECK(recv_buffer_[i],
                        c10::str("Failed to allocate CPU recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = engine_.registerLocalMemory(send_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = engine_.registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                 kWildcardLocation);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
    }

    // Register CPU sync regions
    TORCH_CHECK(size <= kMaxNumRanks, "The number of ranks exceeds the limit.");
    for (size_t i = 0; i < 2; i++) {
        cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(cpu_sync_send_region_[i],
                                             kMaxNumRanks * sizeof(int32_t),
                                             kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks];
        int rc = engine_.registerLocalMemory(cpu_sync_recv_region_[i],
                                             kMaxNumRanks * sizeof(int32_t),
                                             kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    warmup_send_region_ = new int32_t[kMaxNumRanks];
    warmup_send_region_[0] = 1;
    int rc = engine_.registerLocalMemory(
        warmup_send_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);

    warmup_recv_region_ = new int32_t[kMaxNumRanks]{};
    rc = engine_.registerLocalMemory(
        warmup_recv_region_, kMaxNumRanks * sizeof(int32_t), kWildcardLocation);
    TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);

    rank_info.send_buffer[0] = (uint64_t)send_buffer_[0];
    rank_info.send_buffer[1] = (uint64_t)send_buffer_[1];
    rank_info.recv_buffer[0] = (uint64_t)recv_buffer_[0];
    rank_info.recv_buffer[1] = (uint64_t)recv_buffer_[1];
    rank_info.send_sync[0] = (uint64_t)cpu_sync_send_region_[0];
    rank_info.send_sync[1] = (uint64_t)cpu_sync_send_region_[1];
    rank_info.recv_sync[0] = (uint64_t)cpu_sync_recv_region_[0];
    rank_info.recv_sync[1] = (uint64_t)cpu_sync_recv_region_[1];
    rank_info.warmup_buffer[0] = (uint64_t)warmup_send_region_;
    rank_info.warmup_buffer[1] = (uint64_t)warmup_recv_region_;

    std::vector<uint8_t> rank_info_bytes(sizeof(SegmentInfo));
    memcpy(rank_info_bytes.data(), &rank_info, sizeof(SegmentInfo));
    std::string buffer_key =
        "buffer_" + std::to_string(backendIndex_) + "_" + std::to_string(rank_);
    store->set(buffer_key, rank_info_bytes);

    // Sync metadata
    store->set("server_name_" + std::to_string(backendIndex_) + "_" +
                   std::to_string(rank_),
               localServerName);

    int backendIndex = backendIndex_;

    std::thread([this, store, backendIndex] {
        connectionPoller(store, backendIndex);
    }).detach();

    meta_.rank = rank;
    meta_.size = size;
    meta_.taskCount = 0;
    if (isCpu) {
        meta_.activeRanks = new bool[kMaxNumRanks];
    } else {
        cudaHostAlloc(&meta_.activeRanks, kMaxNumRanks * sizeof(bool),
                      cudaHostAllocMapped);
        cudaHostGetDevicePointer(&meta_.activeRanksDevice, meta_.activeRanks,
                                 0);
    }
    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        meta_.activeRanks[i] = true;
    }
    if (options) {
        TORCH_CHECK(options->activeRanks_.dtype() == at::kInt,
                    "activeRanks must be int.");
        if (isCpu) {
            TORCH_CHECK(options->activeRanks_.device().is_cpu(),
                        "activeRanks must be on CPU.");
        } else {
            TORCH_CHECK(options->activeRanks_.device().is_cuda(),
                        "activeRanks must be on CUDA.");
        }
        meta_.activeRanksTensor = options->activeRanks_;
    } else {
        meta_.activeRanksTensor =
            at::ones({size}, torch::dtype(torch::kInt32)
                                 .device(isCpu ? torch::kCPU : torch::kCUDA));
    }
    meta_.engine = &engine_;
    meta_.store = store;
    meta_.backendIndex = backendIndex_;
    meta_.bufferBaseIndex = backendIndex_ * 10;

    while (nextRankForConnection_ != size_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (options && options->isExtension_) {
        auto key = "extension_task_count_" + std::to_string(backendIndex_) +
                   "_" + std::to_string(rank_);
        while (true) {
            if (store->check({key})) {
                meta_.taskCount = std::atoi((char*)store->get(key).data());
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Increment backend index
    ++backendIndex_;

    // Start P2P worker thread for async send/recv operations
    startP2PWorker();
}

MooncakeBackend::~MooncakeBackend() {
    stopP2PWorker();

    if (meta_.activeRanks) {
        if (isCpu_) {
            delete[] meta_.activeRanks;
        } else {
            cudaFreeHost(meta_.activeRanks);
        }
        meta_.activeRanks = nullptr;
    }
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }
namespace {

inline std::string makeP2PCtrlKey(int backendIndex, int src, int dst, int tag,
                                  int64_t seq) {
    return c10::str("p2p_meta_", backendIndex, "_", src, "_", dst, "_", tag,
                    "_", seq);
}

inline std::string makeP2PDoneKey(int backendIndex, int src, int dst, int tag,
                                  int64_t seq) {
    return c10::str("p2p_done_", backendIndex, "_", src, "_", dst, "_", tag,
                    "_", seq);
}

inline std::string makeP2PSlotKey(int backendIndex, int src, int dst, int tag,
                                  int64_t seq) {
    return c10::str("p2p_slot_", backendIndex, "_", src, "_", dst, "_", tag,
                    "_", seq);
}

}  // namespace

c10::intrusive_ptr<c10d::Work> MooncakeBackend::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_.store, "P2P send requires a valid Store.");
    TORCH_CHECK(dstRank >= 0 && dstRank < size_,
                "P2P send: dstRank out of range.");

    auto contiguous = tensor.contiguous();
    const auto numBytes =
        contiguous.numel() * static_cast<size_t>(contiguous.element_size());
    TORCH_CHECK(numBytes <= kBufferSize, "P2P send: tensor size ", numBytes,
                " exceeds total buffer capacity ", kBufferSize, " bytes.");

    const int numSlotsNeeded =
        static_cast<int>((numBytes + kP2PSlotSize - 1) / kP2PSlotSize);
    TORCH_CHECK(numSlotsNeeded <= static_cast<int>(kP2PNumSlots),
                "P2P send: tensor requires ", numSlotsNeeded,
                " slots, but only ", kP2PNumSlots, " slots available.");

    auto completed = std::make_shared<std::atomic<bool>>(false);
    auto errorMsg = std::make_shared<std::string>();

    {
        std::lock_guard<std::mutex> lock(p2pSendQueueMutex_);
        const int64_t seq = meta_.p2pSendSeq[dstRank]++;
        p2pSendQueue_.push(P2POp{.opType = P2POpType::SEND,
                                 .tensor = contiguous,
                                 .originalTensor = at::Tensor(),
                                 .peerRank = dstRank,
                                 .tag = tag,
                                 .seq = seq,
                                 .completed = completed,
                                 .errorMsg = errorMsg});
    }
    p2pSendQueueCv_.notify_one();

    return c10::make_intrusive<MooncakeP2PWork>(completed, errorMsg);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_.store, "P2P recv requires a valid Store.");
    TORCH_CHECK(srcRank >= 0 && srcRank < size_,
                "P2P recv: srcRank out of range.");

    auto target = tensor.is_contiguous() ? tensor : tensor.contiguous();
    const auto expectedBytes =
        target.numel() * static_cast<size_t>(target.element_size());

    auto completed = std::make_shared<std::atomic<bool>>(false);
    auto errorMsg = std::make_shared<std::string>();

    {
        std::lock_guard<std::mutex> lock(p2pRecvQueueMutex_);
        const int64_t seq = meta_.p2pRecvSeq[srcRank]++;
        p2pRecvQueue_.push(P2POp{.opType = P2POpType::RECV,
                                 .tensor = target,
                                 .originalTensor = tensor,
                                 .peerRank = srcRank,
                                 .tag = tag,
                                 .seq = seq,
                                 .completed = completed,
                                 .errorMsg = errorMsg});
    }
    p2pRecvQueueCv_.notify_one();

    return c10::make_intrusive<MooncakeP2PWork>(completed, errorMsg);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::broadcast(
    std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    if (isCpu_) {
        return worker_.putTaskCpu(
            c10d::OpType::BROADCAST, tensorSize, root, &meta_,
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
        return worker_.putTaskCuda(
            c10d::OpType::BROADCAST, tensorSize, root, &meta_, stream,
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
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                memset((char*)tensor.data_ptr() + pos, 0, realSize);
                launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_.activeRanks);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                stream);
                launchReduceKernel(tensor, pos, realSize, src, meta_.size,
                                   opts.reduceOp, meta_.activeRanksDevice,
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
        return worker_.putTaskCpu(
            c10d::OpType::ALLGATHER, tensorSize, 0, &meta_,
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
        return worker_.putTaskCuda(
            c10d::OpType::ALLGATHER, tensorSize, 0, &meta_, stream,
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
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_,
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
        return worker_.putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputBuffer.data_ptr() + pos,
                                realSize, cudaMemcpyDeviceToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_.size)) {
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
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(numRanks)) {
                    memcpy((char*)dst + j * realSize,
                           (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                           realSize);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                memset((char*)outputBuffer.data_ptr() + pos, 0, realSize);
                launchReduceCpu(outputBuffer, pos, realSize, src, numRanks,
                                opts.reduceOp, meta_.activeRanks);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(meta_.size)) {
                    cudaMemcpyAsync(
                        (char*)dst + j * realSize,
                        (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                        realSize, cudaMemcpyDeviceToDevice, stream);
                }
            },
            [=](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)outputBuffer.data_ptr() + pos, 0,
                                realSize, stream);
                launchReduceKernel(outputBuffer, pos, realSize, src, meta_.size,
                                   opts.reduceOp, meta_.activeRanksDevice,
                                   stream);
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions& opts) {
    size_t tensorSize =
        inputTensors[0].numel() * inputTensors[0].element_size();
    if (isCpu_) {
        return worker_.putTaskCpu(
            c10d::OpType::ALLTOALL, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    memcpy(dst + j * realSize,
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
        return worker_.putTaskCuda(
            c10d::OpType::ALLTOALL, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    cudaMemcpyAsync(dst + j * realSize,
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
    TORCH_CHECK(isCpu_, "Barrier is available only for CPU.")
    return worker_.putTaskCpu(
        // a non-zero tensorSize is required to ensure the worker task for the
        // barrier is created
        c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, &meta_,
        [=](void*, size_t, size_t) {}, [=](void*, size_t, size_t) {});
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::reduce(
    std::vector<at::Tensor>& tensors, const c10d::ReduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();
    size_t tensorSize = tensor.numel() * tensor.element_size();
    int64_t root = opts.rootRank + opts.rootTensor;
    bool isRoot = (root == rank_);
    if (isCpu_) {
        auto numRanks = meta_.size;
        return worker_.putTaskCpu(
            c10d::OpType::REDUCE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    memset((char*)tensor.data_ptr() + pos, 0, realSize);
                    launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                    opts.reduceOp, meta_.activeRanks);
                }
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::REDUCE, tensorSize, 0, &meta_, stream,
            [=](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyDeviceToDevice, stream);
            },
            [=](void* src, size_t pos, size_t realSize) {
                if (isRoot) {
                    cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                    stream);
                    launchReduceKernel(tensor, pos, realSize, src, meta_.size,
                                       opts.reduceOp, meta_.activeRanksDevice,
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
        return worker_.putTaskCpu(
            c10d::OpType::GATHER, tensorSize, 0, &meta_,
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
        return worker_.putTaskCuda(
            c10d::OpType::GATHER, tensorSize, 0, &meta_, stream,
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
        return worker_.putTaskCpu(
            c10d::OpType::SCATTER, tensorSize, 0, &meta_,
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
        return worker_.putTaskCuda(
            c10d::OpType::SCATTER, tensorSize, 0, &meta_, stream,
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
    isShutdown_ = true;
    engine_.unregisterLocalMemory(warmup_send_region_);
    engine_.unregisterLocalMemory(warmup_recv_region_);
    delete[] warmup_send_region_;
    delete[] warmup_recv_region_;
    for (size_t i = 0; i < 2; i++) {
        engine_.unregisterLocalMemory(cpu_sync_send_region_[i]);
        engine_.unregisterLocalMemory(cpu_sync_recv_region_[i]);
        engine_.unregisterLocalMemory(send_buffer_[i]);
        engine_.unregisterLocalMemory(recv_buffer_[i]);
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

int MooncakeBackend::getNumSyncedRanks() {
    std::vector<at::Tensor> tensors;
    tensors.emplace_back(torch::tensor(
        nextRankForConnection_,
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

void MooncakeBackend::extendGroupSizeTo(int size) {
    LOG(INFO) << rank_ << " extend to " << size;
    meta_.size = size;
    meta_.taskCount = 0;
    // TODO: compatibility with fault-tolerance
    meta_.activeRanksTensor =
        at::ones({size}, torch::dtype(torch::kInt32)
                             .device(isCpu_ ? torch::kCPU : torch::kCUDA));
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    bool activeRanksBackup[kMaxNumRanks];
    while (true) {
        std::vector<int> input;
        for (const int rank : ranks) {
            input.push_back(meta_.peerConnected[rank]);
        }
        for (int i = 0; i < meta_.size; i++) {
            activeRanksBackup[i] = meta_.activeRanks[i];
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
        for (int i = 0; i < meta_.size; i++) {
            if (activeRanksBackup[i] != meta_.activeRanks[i]) {
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
    for (const int rank : ranks) {
        TORCH_CHECK(meta_.peerConnected[rank]);
        meta_.activeRanks[rank] = true;
        meta_.store->set("extension_task_count_" +
                             std::to_string(meta_.backendIndex) + "_" +
                             std::to_string(rank),
                         std::to_string(meta_.taskCount));
    }
}

void MooncakeBackend::connectionPoller(c10::intrusive_ptr<::c10d::Store> store,
                                       int backendIndex) {
    while (!isShutdown_) {
        for (int pollingRank = 0; pollingRank <= nextRankForConnection_;
             ++pollingRank) {
            if (meta_.peerConnected[pollingRank]) {
                continue;
            }
            std::string serverNameKey = "server_name_" +
                                        std::to_string(backendIndex) + "_" +
                                        std::to_string(pollingRank);
            try {
                if (!store->check({serverNameKey})) {
                    continue;
                }
            } catch (const std::exception& e) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            if (isShutdown_) {
                break;
            }
            auto peerServerName = store->get_to_str(serverNameKey);
            auto segment_id = engine_.openSegment(peerServerName);
            meta_.segmentIDs[pollingRank] = segment_id;
            std::string buffer_key = "buffer_" + std::to_string(backendIndex) +
                                     "_" + std::to_string(pollingRank);
            auto buffer_data = store->get(buffer_key);
            memcpy(&meta_.segmentInfos[pollingRank], buffer_data.data(),
                   sizeof(SegmentInfo));

            if (pollingRank <= rank_) {
                // Send a pre-flight request to establish connections
                std::vector<TransferRequest> entries;
                auto batchID = engine_.allocateBatchID(1);
                engine_.submitTransfer(
                    batchID,
                    {TransferRequest{
                        .opcode = TransferRequest::WRITE,
                        .source = warmup_send_region_,
                        .target_id = meta_.segmentIDs[pollingRank],
                        .target_offset =
                            meta_.segmentInfos[pollingRank].warmup_buffer[1] +
                            rank_ * sizeof(int32_t),
                        .length = sizeof(int32_t),
                    }});

                while (true) {
                    TransferStatus status;
                    engine_.getTransferStatus(batchID, 0, status);
                    if (status.s == TransferStatusEnum::COMPLETED) {
                        break;
                    } else if (status.s == TransferStatusEnum::FAILED) {
                        LOG(WARNING) << "Warmup request " << rank_ << " -> "
                                     << pollingRank << " failed.";
                        break;
                    }
                }
            } else {
                // Wait for the warmup signals
                while (!warmup_recv_region_[pollingRank]) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            meta_.peerConnected[pollingRank] = true;
            if (pollingRank == nextRankForConnection_) {
                ++nextRankForConnection_;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void MooncakeBackend::startP2PWorker() {
    p2pSendWorkerRunning_ = true;
    p2pRecvWorkerRunning_ = true;
    p2pSendWorkerThread_ =
        std::thread(&MooncakeBackend::p2PSendWorkerThread, this);
    p2pRecvWorkerThread_ =
        std::thread(&MooncakeBackend::p2PRecvWorkerThread, this);
}

void MooncakeBackend::stopP2PWorker() {
    if (p2pSendWorkerRunning_.load()) {
        p2pSendWorkerRunning_ = false;
        p2pSendQueueCv_.notify_all();
        if (p2pSendWorkerThread_.joinable()) {
            p2pSendWorkerThread_.join();
        }
    }

    if (p2pRecvWorkerRunning_.load()) {
        p2pRecvWorkerRunning_ = false;
        p2pRecvQueueCv_.notify_all();
        if (p2pRecvWorkerThread_.joinable()) {
            p2pRecvWorkerThread_.join();
        }
    }
}

void MooncakeBackend::p2PSendWorkerThread() {
    while (p2pSendWorkerRunning_.load()) {
        P2POp op;
        {
            std::unique_lock<std::mutex> lock(p2pSendQueueMutex_);
            p2pSendQueueCv_.wait(lock, [this] {
                return !p2pSendQueue_.empty() || !p2pSendWorkerRunning_.load();
            });

            if (!p2pSendWorkerRunning_.load() && p2pSendQueue_.empty()) {
                break;
            }

            if (p2pSendQueue_.empty()) {
                continue;
            }

            op = p2pSendQueue_.front();
            p2pSendQueue_.pop();
        }

        try {
            processSendOp(op);
            op.completed->store(true, std::memory_order_release);
        } catch (const std::exception& e) {
            *op.errorMsg = e.what();
            op.completed->store(true, std::memory_order_release);
        }
    }
}

void MooncakeBackend::p2PRecvWorkerThread() {
    while (p2pRecvWorkerRunning_.load()) {
        P2POp op;
        bool foundReady = false;
        {
            std::unique_lock<std::mutex> lock(p2pRecvQueueMutex_);
            p2pRecvQueueCv_.wait(lock, [this] {
                return !p2pRecvQueue_.empty() || !p2pRecvWorkerRunning_.load();
            });

            if (!p2pRecvWorkerRunning_.load() && p2pRecvQueue_.empty()) {
                break;
            }

            if (p2pRecvQueue_.empty()) {
                continue;
            }

            std::queue<P2POp> tempQueue;
            while (!p2pRecvQueue_.empty()) {
                P2POp candidate = p2pRecvQueue_.front();
                p2pRecvQueue_.pop();

                if (!foundReady &&
                    candidate.seq ==
                        meta_.p2pRecvNextExpected[candidate.peerRank]) {
                    op = candidate;
                    foundReady = true;
                } else {
                    tempQueue.push(candidate);
                }
            }

            // Put remaining operations back
            while (!tempQueue.empty()) {
                p2pRecvQueue_.push(tempQueue.front());
                tempQueue.pop();
            }
        }

        if (foundReady) {
            try {
                processRecvOp(op);
                meta_.p2pRecvNextExpected[op.peerRank] = op.seq + 1;
                op.completed->store(true, std::memory_order_release);
            } catch (const std::exception& e) {
                *op.errorMsg = e.what();
                op.completed->store(true, std::memory_order_release);
            }
        } else {
            // No ready operation, wait a bit and retry
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
}

void MooncakeBackend::processSendOp(const P2POp& op) {
    auto tensor = op.tensor;
    int dstRank = op.peerRank;
    int tag = op.tag;
    int64_t seq = op.seq;

    const auto numBytes =
        tensor.numel() * static_cast<size_t>(tensor.element_size());

    const int numSlotsNeeded =
        static_cast<int>((numBytes + kP2PSlotSize - 1) / kP2PSlotSize);

    const std::string slotRequestKey =
        makeP2PSlotKey(meta_.backendIndex, rank_, dstRank, tag, seq);
    meta_.store->set(slotRequestKey, c10::str(numSlotsNeeded, "_", numBytes));

    std::vector<std::string> keys = {slotRequestKey};
    int baseSlot = 0;
    int allocatedSlots = 0;
    while (true) {
        if (meta_.store->check(keys)) {
            auto slotValue = meta_.store->get(slotRequestKey);
            std::string slotStr(slotValue.begin(), slotValue.end());
            size_t firstUnderscore = slotStr.find('_');
            if (firstUnderscore != std::string::npos) {
                try {
                    int firstNum =
                        std::stoi(slotStr.substr(0, firstUnderscore));
                    int secondNum =
                        std::stoi(slotStr.substr(firstUnderscore + 1));
                    if (secondNum <= static_cast<int>(kP2PNumSlots) &&
                        firstNum < static_cast<int>(kP2PNumSlots) &&
                        secondNum == numSlotsNeeded) {
                        baseSlot = firstNum;
                        allocatedSlots = secondNum;
                        break;
                    }
                } catch (const std::exception&) {
                    // Invalid format, keep waiting.
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    const std::string ctrlKey =
        makeP2PCtrlKey(meta_.backendIndex, rank_, dstRank, tag, seq);
    uint64_t sendAddrBase = meta_.segmentInfos[rank_].send_buffer[0];

    uint64_t sendAddr = sendAddrBase + baseSlot * kP2PSlotSize;
    void* sendBuf = reinterpret_cast<void*>(sendAddr);

    if (isCpu_) {
        std::memcpy(sendBuf, tensor.data_ptr(), numBytes);
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto err = cudaMemcpyAsync(sendBuf, tensor.data_ptr(), numBytes,
                                   cudaMemcpyDeviceToDevice, stream);
        TORCH_CHECK(
            !err, "P2P send cudaMemcpyAsync failed: ", cudaGetErrorString(err));
        cudaStreamSynchronize(stream);
    }
    uint64_t remoteRecvAddrBase = meta_.segmentInfos[dstRank].recv_buffer[0];

    uint64_t remoteRecvAddr = remoteRecvAddrBase + baseSlot * kP2PSlotSize;
    std::vector<TransferRequest> entries;
    entries.push_back(TransferRequest{
        .opcode = TransferRequest::WRITE,
        .source = sendBuf,
        .target_id = meta_.segmentIDs[dstRank],
        .target_offset = remoteRecvAddr,
        .length = numBytes,
    });
    auto batchID = meta_.engine->allocateBatchID(entries.size());
    meta_.engine->submitTransfer(batchID, entries);

    TransferStatus status;
    while (true) {
        meta_.engine->getTransferStatus(batchID, 0, status);
        if (status.s == TransferStatusEnum::COMPLETED) {
            break;
        }
        TORCH_CHECK(status.s != TransferStatusEnum::FAILED,
                    "P2P send transfer failed.");
    }

    meta_.store->set(ctrlKey,
                     c10::str(baseSlot, "_", allocatedSlots, "_", numBytes));
}

void MooncakeBackend::processRecvOp(const P2POp& op) {
    auto tensor = op.tensor;
    int srcRank = op.peerRank;
    int tag = op.tag;
    int64_t seq = op.seq;

    const auto expectedBytes =
        tensor.numel() * static_cast<size_t>(tensor.element_size());

    int baseSlot = static_cast<int>(seq % kP2PNumSlots);

    const std::string slotRequestKey =
        makeP2PSlotKey(meta_.backendIndex, srcRank, rank_, tag, seq);
    std::vector<std::string> keys = {slotRequestKey};
    while (!meta_.store->check(keys)) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    auto requestValue = meta_.store->get(slotRequestKey);
    std::string requestStr(requestValue.begin(), requestValue.end());
    int numSlotsNeeded = 0;
    size_t numBytes = 0;
    try {
        size_t firstUnderscore = requestStr.find('_');
        TORCH_CHECK(firstUnderscore != std::string::npos,
                    "P2P recv: invalid slot request format: ", requestStr);
        numSlotsNeeded = std::stoi(requestStr.substr(0, firstUnderscore));
        numBytes = static_cast<size_t>(
            std::stoull(requestStr.substr(firstUnderscore + 1)));
    } catch (const std::exception& e) {
        TORCH_CHECK(false,
                    "P2P recv: failed to parse slot request: ", e.what());
    }

    if (baseSlot + numSlotsNeeded > static_cast<int>(kP2PNumSlots)) {
        baseSlot = 0;
    }

    while (seq >= meta_.p2pRecvLowestInFlight[srcRank] + kP2PNumSlots) {
        const int64_t waitSeq = meta_.p2pRecvLowestInFlight[srcRank];
        const std::string doneKey =
            makeP2PDoneKey(meta_.backendIndex, srcRank, rank_, tag, waitSeq);
        std::vector<std::string> doneKeys = {doneKey};
        while (!meta_.store->check(doneKeys)) {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        meta_.store->get(doneKey);
        ++meta_.p2pRecvLowestInFlight[srcRank];
    }

    meta_.store->set(slotRequestKey, c10::str(baseSlot, "_", numSlotsNeeded));

    const std::string ctrlKey =
        makeP2PCtrlKey(meta_.backendIndex, srcRank, rank_, tag, seq);

    std::vector<std::string> ctrlKeys = {ctrlKey};
    while (!meta_.store->check(ctrlKeys)) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    auto ctrlValue = meta_.store->get(ctrlKey);
    std::string ctrlStr(ctrlValue.begin(), ctrlValue.end());
    int numSlots = 0;
    try {
        size_t firstUnderscore = ctrlStr.find('_');
        TORCH_CHECK(firstUnderscore != std::string::npos,
                    "P2P recv: invalid control message format: ", ctrlStr);
        int ctrlBaseSlot = std::stoi(ctrlStr.substr(0, firstUnderscore));
        TORCH_CHECK(ctrlBaseSlot == baseSlot,
                    "P2P recv: slot mismatch, expected ", baseSlot,
                    " but sender reported ", ctrlBaseSlot);
        size_t secondUnderscore = ctrlStr.find('_', firstUnderscore + 1);
        TORCH_CHECK(secondUnderscore != std::string::npos,
                    "P2P recv: invalid control message format: ", ctrlStr);
        numSlots = std::stoi(ctrlStr.substr(
            firstUnderscore + 1, secondUnderscore - firstUnderscore - 1));
        size_t ctrlNumBytes = static_cast<size_t>(
            std::stoull(ctrlStr.substr(secondUnderscore + 1)));
        TORCH_CHECK(ctrlNumBytes == numBytes,
                    "P2P recv: byte size mismatch, expected ", numBytes,
                    " but sender reported ", ctrlNumBytes);
    } catch (const std::exception& e) {
        TORCH_CHECK(false,
                    "P2P recv: failed to parse control message: ", e.what());
    }
    TORCH_CHECK(numBytes == expectedBytes,
                "P2P recv: tensor byte size mismatch for key ", ctrlKey,
                ", expected ", expectedBytes, " bytes but got ", numBytes,
                " bytes.");
    TORCH_CHECK(baseSlot >= 0 && baseSlot < static_cast<int>(kP2PNumSlots),
                "P2P recv: invalid base slot index: ", baseSlot);
    TORCH_CHECK(
        numSlots > 0 && baseSlot + numSlots <= static_cast<int>(kP2PNumSlots),
        "P2P recv: invalid slot range: baseSlot=", baseSlot,
        ", numSlots=", numSlots);
    TORCH_CHECK(numSlots == numSlotsNeeded,
                "P2P recv: slot count mismatch, expected ", numSlotsNeeded,
                " but got ", numSlots);
    uint64_t recvAddrBase = meta_.segmentInfos[rank_].recv_buffer[0];

    uint64_t recvAddr = recvAddrBase + baseSlot * kP2PSlotSize;
    void* recvBuf = reinterpret_cast<void*>(recvAddr);

    if (isCpu_) {
        std::memcpy(tensor.data_ptr(), recvBuf, numBytes);
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto err = cudaMemcpyAsync(tensor.data_ptr(), recvBuf, numBytes,
                                   cudaMemcpyDeviceToDevice, stream);
        TORCH_CHECK(
            !err, "P2P recv cudaMemcpyAsync failed: ", cudaGetErrorString(err));
        cudaStreamSynchronize(stream);
    }

    if (!op.originalTensor.is_contiguous()) {
        op.originalTensor.copy_(tensor);
    }

    const std::string doneKey =
        makeP2PDoneKey(meta_.backendIndex, srcRank, rank_, tag, seq);
    meta_.store->set(doneKey, "1");

    if (seq == meta_.p2pRecvLowestInFlight[srcRank]) {
        ++meta_.p2pRecvLowestInFlight[srcRank];
    }
}
}  // namespace mooncake
