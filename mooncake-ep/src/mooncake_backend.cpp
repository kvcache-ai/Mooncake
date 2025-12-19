#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <thread>
#include <chrono>

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
Transport* MooncakeBackend::transport_ = nullptr;
int MooncakeBackend::backendIndex_ = 0;
MooncakeWorker MooncakeBackend::worker_;

namespace {

// Simple Work implementation for the synchronous P2P helpers. The
// underlying send/recv complete before the Work object is created, so
// wait/isCompleted trivially succeed.
class MooncakeP2PWork : public ::c10d::Work {
   public:
    MooncakeP2PWork() : Work(-1, c10d::OpType::UNKNOWN) {}

    bool isCompleted() override { return true; }

    bool wait(std::chrono::milliseconds /*timeout*/) override { return true; }
};

}  // namespace

MooncakeBackend::MooncakeBackend(
    c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : Backend(rank, size), isCpu_(isCpu) {
    // Initialize transfer engine
    if (!transport_) {
        engine_.init(P2PHANDSHAKE, hostIp_);
        transport_ = engine_.installTransport("rdma", nullptr);
        if (!transport_) {
            // Fallback to TCP
            transport_ = engine_.installTransport("tcp", nullptr);
            TORCH_CHECK(transport_ != nullptr,
                        c10::str("Failed to install transport"));
            LOG(WARNING) << "[Torch Backend] RDMA transport unavailable. "
                            "Fallback to TCP.";
        }
    }
    auto localRpcMeta = transport_->meta()->localRpcMeta();
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

    // Reset the synchronization signal
    store->deleteKey("backend_init_" + std::to_string(backendIndex_) + "_" +
                     std::to_string(rank_));
    store->deleteKey("backend_warmup_" + std::to_string(backendIndex_) + "_" +
                     std::to_string(rank_));

    // Sync metadata
    store->set("server_name_" + std::to_string(backendIndex_) + "_" +
                   std::to_string(rank_),
               localServerName);

    std::vector<std::string> server_names;
    for (int i = 0; i < size; i++) {
        server_names.push_back(store->get_to_str("server_name_" +
                                                 std::to_string(backendIndex_) +
                                                 "_" + std::to_string(i)));
    }

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
    meta_.bufferBaseIndex = backendIndex_ * 8;
    meta_.segmentIDs.clear();
    meta_.segmentDescs.clear();
    for (int i = 0; i < size_; ++i) {
        auto segment_id = engine_.openSegment(server_names[i]);
        meta_.segmentIDs.emplace_back(segment_id);
        auto segment_desc =
            engine_.getMetadata()->getSegmentDescByID(segment_id, true);
        meta_.segmentDescs.emplace_back(segment_desc);
    }

    // Let the default process group warm up the transfer engine
    if (backendIndex_ == 0) {
        std::vector<TransferRequest> entries;
        for (int i = rank_; i < size_; ++i) {
            entries.push_back(TransferRequest{
                .opcode = TransferRequest::READ,
                .source =
                    (int32_t*)meta_.segmentDescs[rank_]->buffers[4].addr + 1,
                .target_id = meta_.segmentIDs[i],
                .target_offset = meta_.segmentDescs[i]->buffers[6].addr,
                .length = sizeof(int32_t),
            });
        }
        store->set("backend_warmup_" + std::to_string(backendIndex_) + "_" +
                       std::to_string(rank_),
                   "1");
        // Ensure all backends have received peer data
        for (int i = 0; i < size_; i++) {
            store->get_to_str("backend_warmup_" +
                              std::to_string(backendIndex_) + "_" +
                              std::to_string(i));
        }
        auto batchID = engine_.allocateBatchID(entries.size());
        engine_.submitTransfer(batchID, entries);

        while (true) {
            bool batch_done = true;
            TransferStatus status;
            for (int i = 0; i < size_ - rank_; ++i) {
                engine_.getTransferStatus(batchID, i, status);
                if (status.s != TransferStatusEnum::COMPLETED &&
                    status.s != TransferStatusEnum::FAILED) {
                    batch_done = false;
                    break;
                }
            }
            if (batch_done) {
                break;
            }
        }
    }
    store->set("backend_init_" + std::to_string(backendIndex_) + "_" +
                   std::to_string(rank_),
               "1");

    // Ensure that all ranks have been initialized
    for (int i = 0; i < size_; i++) {
        store->get_to_str("backend_init_" + std::to_string(backendIndex_) +
                          "_" + std::to_string(i));
    }

    store->deleteKey("server_name_" + std::to_string(backendIndex_) + "_" +
                     std::to_string(rank_));

    // Increment backend index
    ++backendIndex_;
}

MooncakeBackend::~MooncakeBackend() {
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
    TORCH_CHECK(numBytes <= kP2PSlotSize,
                "P2P send: tensor size ", numBytes,
                " exceeds per-slot capacity ", kP2PSlotSize, " bytes.");

    const auto seq = meta_.p2pSendSeq[dstRank]++;
    const int slot = static_cast<int>(seq % kP2PNumSlots);

    // Wait for slot to be available if needed (check if we've wrapped around).
    if (seq >= meta_.p2pSendLowestInFlight[dstRank] + kP2PNumSlots) {
        // Need to wait for the oldest in-flight message to complete.
        const int64_t waitSeq = meta_.p2pSendLowestInFlight[dstRank];
        const std::string doneKey = makeP2PDoneKey(meta_.backendIndex, rank_,
                                                   dstRank, tag, waitSeq);
        // Poll until the receiver confirms completion.
        std::vector<std::string> keys = {doneKey};
        while (!meta_.store->check(keys)) {
            // Brief sleep to avoid busy-waiting.
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        // Key exists, now get it to ensure it's fully written.
        meta_.store->get(doneKey);
        ++meta_.p2pSendLowestInFlight[dstRank];
    }

    const std::string ctrlKey =
        makeP2PCtrlKey(meta_.backendIndex, rank_, dstRank, tag, seq);

    const int bufferIndex = meta_.bufferBaseIndex;  // use slot 0
    uint64_t sendAddrBase =
        meta_.segmentDescs[rank_]->buffers[bufferIndex].addr;
    uint64_t sendAddr = sendAddrBase + slot * kP2PSlotSize;
    void* sendBuf = reinterpret_cast<void*>(sendAddr);

    if (isCpu_) {
        TORCH_CHECK(tensor.device().is_cpu(),
                    "P2P send expects a CPU tensor for mooncake-cpu backend.");
        std::memcpy(sendBuf, contiguous.data_ptr(), numBytes);
    } else {
        TORCH_CHECK(
            tensor.device().is_cuda(),
            "P2P send expects a CUDA tensor for the mooncake (GPU) backend.");
        auto stream =
            at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto err = cudaMemcpyAsync(sendBuf, contiguous.data_ptr(), numBytes,
                                   cudaMemcpyDeviceToDevice, stream);
        TORCH_CHECK(!err, "P2P send cudaMemcpyAsync failed: ",
                    cudaGetErrorString(err));
        cudaStreamSynchronize(stream);
    }

    // RDMA write from local send buffer slot to peer recv buffer slot.
    uint64_t remoteRecvAddrBase =
        meta_.segmentDescs[dstRank]->buffers[bufferIndex + 2].addr;
    uint64_t remoteRecvAddr = remoteRecvAddrBase + slot * kP2PSlotSize;
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

    // Publish control metadata (slot, size) after data is visible remotely.
    meta_.store->set(ctrlKey, c10::str(slot, "_", numBytes));

    return c10::make_intrusive<MooncakeP2PWork>();
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_.store, "P2P recv requires a valid Store.");
    TORCH_CHECK(srcRank >= 0 && srcRank < size_,
                "P2P recv: srcRank out of range.");

    auto target =
        tensor.is_contiguous() ? tensor : tensor.contiguous();
    const auto expectedBytes =
        target.numel() * static_cast<size_t>(target.element_size());

    const auto seq = meta_.p2pRecvSeq[srcRank]++;
    const std::string ctrlKey =
        makeP2PCtrlKey(meta_.backendIndex, srcRank, rank_, tag, seq);

    // Poll for control message with slot index and size.
    std::vector<std::string> keys = {ctrlKey};
    while (!meta_.store->check(keys)) {
        // Brief sleep to avoid busy-waiting.
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    // Key exists, now get it.
    auto ctrlValue = meta_.store->get(ctrlKey);
    std::string ctrlStr(ctrlValue.begin(), ctrlValue.end());
    size_t numBytes = 0;
    int slot = 0;
    try {
        // Parse "slot_size" format.
        size_t underscorePos = ctrlStr.find('_');
        TORCH_CHECK(underscorePos != std::string::npos,
                    "P2P recv: invalid control message format: ", ctrlStr);
        slot = std::stoi(ctrlStr.substr(0, underscorePos));
        numBytes = static_cast<size_t>(
            std::stoull(ctrlStr.substr(underscorePos + 1)));
    } catch (const std::exception& e) {
        TORCH_CHECK(false,
                    "P2P recv: failed to parse control message: ", e.what());
    }
    TORCH_CHECK(numBytes == expectedBytes,
                "P2P recv: tensor byte size mismatch for key ", ctrlKey,
                ", expected ", expectedBytes, " bytes but got ", numBytes,
                " bytes.");
    TORCH_CHECK(slot >= 0 && slot < static_cast<int>(kP2PNumSlots),
                "P2P recv: invalid slot index: ", slot);

    const int bufferIndex = meta_.bufferBaseIndex;  // slot 0
    uint64_t recvAddrBase =
        meta_.segmentDescs[rank_]->buffers[bufferIndex + 2].addr;
    uint64_t recvAddr = recvAddrBase + slot * kP2PSlotSize;
    void* recvBuf = reinterpret_cast<void*>(recvAddr);

    if (isCpu_) {
        TORCH_CHECK(tensor.device().is_cpu(),
                    "P2P recv expects a CPU tensor for mooncake-cpu backend.");
        std::memcpy(target.data_ptr(), recvBuf, numBytes);
    } else {
        TORCH_CHECK(
            tensor.device().is_cuda(),
            "P2P recv expects a CUDA tensor for the mooncake (GPU) backend.");
        auto stream =
            at::cuda::getCurrentCUDAStream(tensor.device().index());
        auto err = cudaMemcpyAsync(target.data_ptr(), recvBuf, numBytes,
                                   cudaMemcpyDeviceToDevice, stream);
        TORCH_CHECK(!err, "P2P recv cudaMemcpyAsync failed: ",
                    cudaGetErrorString(err));
        cudaStreamSynchronize(stream);
    }

    if (!tensor.is_contiguous()) {
        tensor.copy_(target);
    }

    // Notify sender that this slot is now free (after we've consumed the data).
    const std::string doneKey =
        makeP2PDoneKey(meta_.backendIndex, srcRank, rank_, tag, seq);
    meta_.store->set(doneKey, "1");

    // Update lowest in-flight sequence if we've consumed the oldest one.
    if (seq == meta_.p2pRecvLowestInFlight[srcRank]) {
        ++meta_.p2pRecvLowestInFlight[srcRank];
    }

    return c10::make_intrusive<MooncakeP2PWork>();
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
            [&](void* dst, size_t pos, size_t realSize) {
                if (isRoot) {
                    cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos,
                                    realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [&](void* src, size_t pos, size_t realSize) {
                cudaMemcpyAsync((char*)tensor.data_ptr() + pos, src, realSize,
                                cudaMemcpyDeviceToHost, stream);
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
        auto numRanks = size_;
        return worker_.putTaskCpu(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_,
            [=](void* dst, size_t pos, size_t realSize) {
                memcpy(dst, (char*)tensor.data_ptr() + pos, realSize);
            },
            [=](void* src, size_t pos, size_t realSize) {
                memset((char*)tensor.data_ptr() + pos, 0, realSize);
                launchReduceCpu(tensor, pos, realSize, src, numRanks,
                                opts.reduceOp);
            });
    } else {
        auto stream = at::cuda::getCurrentCUDAStream(tensor.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, &meta_, stream,
            [&](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)tensor.data_ptr() + pos, realSize,
                                cudaMemcpyHostToDevice, stream);
            },
            [&](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)tensor.data_ptr() + pos, 0, realSize,
                                stream);
                launchReduceKernel(tensor, pos, realSize, src, size_,
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
            [&](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputTensor.data_ptr() + pos,
                                realSize, cudaMemcpyHostToDevice, stream);
            },
            [&](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors_.size())) {
                    cudaMemcpyAsync((char*)outputTensors_[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToHost, stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions& opts) {
    size_t tensorSize = inputBuffer.numel() * inputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = size_;
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
            [&](void* dst, size_t pos, size_t realSize) {
                cudaMemcpyAsync(dst, (char*)inputBuffer.data_ptr() + pos,
                                realSize, cudaMemcpyHostToDevice, stream);
            },
            [&](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(size_)) {
                    cudaMemcpyAsync(
                        (char*)outputBuffer.data_ptr() + j * tensorSize + pos,
                        (char*)src + j * realSize, realSize,
                        cudaMemcpyDeviceToHost, stream);
                }
            });
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    size_t tensorSize = outputBuffer.numel() * outputBuffer.element_size();
    if (isCpu_) {
        auto numRanks = size_;
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
                                opts.reduceOp);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        return worker_.putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, &meta_, stream,
            [&](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(size_)) {
                    cudaMemcpyAsync(
                        (char*)dst + j * realSize,
                        (char*)inputBuffer.data_ptr() + j * tensorSize + pos,
                        realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [&](void* src, size_t pos, size_t realSize) {
                cudaMemsetAsync((char*)outputBuffer.data_ptr() + pos, 0,
                                realSize, stream);
                launchReduceKernel(outputBuffer, pos, realSize, src, size_,
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
            [&](void* dst, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(inputTensors.size())) {
                    cudaMemcpyAsync(dst + j * realSize,
                                    (char*)inputTensors[j].data_ptr() + pos,
                                    realSize, cudaMemcpyHostToDevice, stream);
                }
            },
            [&](void* src, size_t pos, size_t realSize) {
                for (const auto j : c10::irange(outputTensors.size())) {
                    cudaMemcpyAsync((char*)outputTensors[j].data_ptr() + pos,
                                    (char*)src + j * realSize, realSize,
                                    cudaMemcpyDeviceToHost, stream);
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

void MooncakeBackend::shutdown() {
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
    --backendIndex_;
}
}  // namespace mooncake
