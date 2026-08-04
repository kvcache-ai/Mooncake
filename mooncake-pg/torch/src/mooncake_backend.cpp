#include <mooncake_backend.h>
#include <torch_utils.h>

#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDAGraphsUtils.cuh>
#include <c10/cuda/CUDAStream.h>
#include <c10/util/env.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>

#include <exception>
#include <functional>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

constexpr const char* kSingleTensorError =
    "Expecting one tensor only but got multiple.";
constexpr const char* kSparseError = "Sparse op not supported.";
mooncakePgDataType_t tensorType(const at::Tensor& tensor) {
    switch (tensor.scalar_type()) {
        case at::kChar:
            return mooncakePgInt8;
        case at::kByte:
            return mooncakePgUint8;
        case at::kShort:
            return mooncakePgInt16;
        case at::kUInt16:
            return mooncakePgUint16;
        case at::kInt:
            return mooncakePgInt32;
        case at::kUInt32:
            return mooncakePgUint32;
        case at::kLong:
            return mooncakePgInt64;
        case at::kUInt64:
            return mooncakePgUint64;
        case at::kHalf:
            return mooncakePgFloat16;
        case at::kFloat:
            return mooncakePgFloat32;
        case at::kDouble:
            return mooncakePgFloat64;
        case at::kBool:
            return mooncakePgBool;
        case at::kBFloat16:
            return mooncakePgBfloat16;
        case at::kFloat8_e4m3fn:
            return mooncakePgFloat8e4m3fn;
        case at::kFloat8_e5m2:
            return mooncakePgFloat8e5m2;
        case at::kFloat8_e4m3fnuz:
            return mooncakePgFloat8e4m3fnuz;
        case at::kFloat8_e5m2fnuz:
            return mooncakePgFloat8e5m2fnuz;
        case at::kFloat8_e8m0fnu:
            return mooncakePgFloat8e8m0fnu;
        default:
            TORCH_CHECK(false, "Unsupported Mooncake PG datatype: ",
                        tensor.scalar_type());
    }
}

mooncakePgReduceOp_t convertReduceOp(const c10d::ReduceOp& reduce_op) {
    switch (reduce_op) {
        case c10d::ReduceOp::SUM:
            return mooncakePgSum;
        case c10d::ReduceOp::AVG:
            return mooncakePgAvg;
        case c10d::ReduceOp::PRODUCT:
            return mooncakePgProduct;
        case c10d::ReduceOp::MIN:
            return mooncakePgMin;
        case c10d::ReduceOp::MAX:
            return mooncakePgMax;
        default:
            TORCH_CHECK(false, "Unsupported Mooncake PG op: ", reduce_op);
    }
}

size_t tensorCount(const at::Tensor& tensor) {
    TORCH_CHECK(tensor.numel() >= 0, "invalid Tensor element count");
    return static_cast<size_t>(tensor.numel());
}

c10::cuda::CUDAStream currentCudaStream(const at::Tensor& tensor) {
    return c10::cuda::getCurrentCUDAStream(tensor.device().index());
}

mooncakePgStream_t convertStream(const c10::cuda::CUDAStream& stream) {
    return reinterpret_cast<mooncakePgStream_t>(stream.stream());
}

void validateEqualPeerTensors(const std::vector<at::Tensor>& tensors,
                              const at::Tensor& reference, int active_size) {
    TORCH_CHECK(tensors.size() == static_cast<size_t>(active_size),
                "Tensor list size must match active group size");
    for (const auto& tensor : tensors) {
        TORCH_CHECK(tensor.scalar_type() == reference.scalar_type(),
                    "All peer tensors must have the same dtype");
        TORCH_CHECK(tensor.device() == reference.device(),
                    "All peer tensors must be on the same device");
        TORCH_CHECK(tensor.numel() == reference.numel(),
                    "All peer tensors must have the same number of elements");
    }
}

void validateSingleBufferTensors(const at::Tensor& output,
                                 const at::Tensor& input,
                                 c10::DeviceType expected_device) {
    TORCH_CHECK(input.device().type() == expected_device,
                "Input tensor device does not match the backend device");
    TORCH_CHECK(output.device() == input.device(),
                "Input and output tensors must be on the same device");
    TORCH_CHECK(output.scalar_type() == input.scalar_type(),
                "Input and output tensors must have the same dtype");
    TORCH_CHECK(input.is_contiguous(), "Input tensor must be contiguous");
    TORCH_CHECK(output.is_contiguous(), "Output tensor must be contiguous");
}

at::Tensor packPeerTensors(const std::vector<at::Tensor>& tensors,
                           const at::Tensor& reference, int active_size) {
    validateEqualPeerTensors(tensors, reference, active_size);
    const int64_t elements_per_peer = reference.numel();
    auto packed =
        at::empty({elements_per_peer * static_cast<int64_t>(tensors.size())},
                  reference.options());
    for (size_t index = 0; index < tensors.size(); ++index) {
        packed
            .narrow(0, static_cast<int64_t>(index) * elements_per_peer,
                    elements_per_peer)
            .copy_(tensors[index].reshape({elements_per_peer}));
    }
    return packed;
}

std::function<void()> makeCopyBackToPeerTensors(
    at::Tensor packed, std::vector<at::Tensor> outputs) {
    return [packed = std::move(packed),
            outputs = std::move(outputs)]() mutable {
        if (outputs.empty()) return;
        const int64_t elements_per_peer = outputs.front().numel();
        for (size_t index = 0; index < outputs.size(); ++index) {
            outputs[index].copy_(
                packed
                    .narrow(0, static_cast<int64_t>(index) * elements_per_peer,
                            elements_per_peer)
                    .view(outputs[index].sizes()));
        }
    };
}

std::vector<int32_t> convertRanks(const std::vector<int>& ranks) {
    return std::vector<int32_t>(ranks.begin(), ranks.end());
}

// Lightweight Backend shim that delegates operations back to the owning
// MooncakeBackend. PyTorch's P2P dispatch (batch_isend_irecv, isend, irecv)
// requires getBackend() to return a registered c10d::Backend instance.
// Since MooncakeBackend inherits from ProcessGroup (not Backend), we register
// this shim in the ProcessGroup's deviceTypeToBackend_ map. The shim holds a
// non-owning pointer to its owner.
//
// PyTorch 2.13 added ProcessGroup::all_gather_single and
// ProcessGroup::reduce_scatter_single, and the deprecated single-buffer
// aliases now forward to those methods. They dispatch through c10d/Ops.cpp
// and ProcessGroup::getBackend(dev), so calls land on this registered shim
// instead of MooncakeBackend's _allgather_base and _reduce_scatter_base
// overrides. Delegate every collective MooncakeBackend implements so the
// shim exposes the same capabilities as its owner.
class MooncakeBackendShim final : public ::c10d::Backend {
   public:
    MooncakeBackendShim(MooncakeBackend* owner, int maxGroupSize)
        : Backend(owner->getRank(), maxGroupSize), owner_(owner) {}

    const std::string getBackendName() const override { return "mooncake"; }
    bool supportsCoalescing() const override { return false; }

    c10::intrusive_ptr<c10d::Work> send(std::vector<at::Tensor>& tensors,
                                        int dstRank, int tag) override {
        return owner_->send(tensors, dstRank, tag);
    }

    c10::intrusive_ptr<c10d::Work> recv(std::vector<at::Tensor>& tensors,
                                        int srcRank, int tag) override {
        return owner_->recv(tensors, srcRank, tag);
    }

    c10::intrusive_ptr<c10d::Work> recvAnysource(
        std::vector<at::Tensor>& tensors, int tag) override {
        // MooncakeBackend doesn't implement recvAnysource; fall back to the
        // base class which will raise a clear error.
        return ::c10d::Backend::recvAnysource(tensors, tag);
    }

    c10::intrusive_ptr<c10d::Work> barrier(
        const c10d::BarrierOptions& opts) override {
        return owner_->barrier(opts);
    }

    // Signatures mirror MooncakeBackend's overrides so the shim re-exposes the
    // same c10d virtuals.
    c10::intrusive_ptr<c10d::Work> broadcast(
        std::vector<at::Tensor>& tensors,
        const c10d::BroadcastOptions& opts) override {
        return owner_->broadcast(tensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> allreduce(
        std::vector<at::Tensor>& tensors,
        const c10d::AllreduceOptions& opts) override {
        return owner_->allreduce(tensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> allgather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllgatherOptions& opts) override {
        return owner_->allgather(outputTensors, inputTensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> _allgather_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::AllgatherOptions& opts) override {
        return owner_->_allgather_base(outputBuffer, inputBuffer, opts);
    }

    c10::intrusive_ptr<c10d::Work> _reduce_scatter_base(
        at::Tensor& outputBuffer, at::Tensor& inputBuffer,
        const c10d::ReduceScatterOptions& opts) override {
        return owner_->_reduce_scatter_base(outputBuffer, inputBuffer, opts);
    }

    c10::intrusive_ptr<c10d::Work> alltoall(
        std::vector<at::Tensor>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::AllToAllOptions& opts) override {
        return owner_->alltoall(outputTensors, inputTensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> reduce(
        std::vector<at::Tensor>& tensors,
        const c10d::ReduceOptions& opts) override {
        return owner_->reduce(tensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> gather(
        std::vector<std::vector<at::Tensor>>& outputTensors,
        std::vector<at::Tensor>& inputTensors,
        const c10d::GatherOptions& opts) override {
        return owner_->gather(outputTensors, inputTensors, opts);
    }

    c10::intrusive_ptr<c10d::Work> scatter(
        std::vector<at::Tensor>& outputTensors,
        std::vector<std::vector<at::Tensor>>& inputTensors,
        const c10d::ScatterOptions& opts) override {
        return owner_->scatter(outputTensors, inputTensors, opts);
    }

   private:
    // Non-owning: the shim is stored in ProcessGroup's backend maps which are
    // cleared on destruction, and MooncakeBackend always outlives the shim.
    MooncakeBackend* owner_;
};

}  // namespace

/**
 * @brief Initialize Mooncake backend state from the PyTorch process-group
 * information and optional Mooncake-specific options.
 */
MooncakeBackend::MooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackendOptions> options,
    mooncakePgContext_t context, bool isCpu)
    : ProcessGroup(distBackendOpts.store, distBackendOpts.group_rank,
                   distBackendOpts.group_size),
      options_(std::move(options)),
      isCpu_(isCpu),
      work_tracker_(std::make_shared<MooncakeWorkTracker>()) {
    TORCH_CHECK(context, "Mooncake PG core context is null");

    const int rank = distBackendOpts.group_rank;
    const int size = distBackendOpts.group_size;
    max_group_size_ = options_ && options_->maxGroupSize_ > 0
                          ? options_->maxGroupSize_
                          : size;
    TORCH_CHECK(max_group_size_ >= size && max_group_size_ > 0 &&
                    max_group_size_ <= MOONCAKE_PG_MAX_RANKS,
                "max_group_size must be in [group_size, ",
                MOONCAKE_PG_MAX_RANKS, "]");
    TORCH_CHECK(rank >= 0 && rank < size, "rank out of valid range");
    TORCH_CHECK(!distBackendOpts.group_id.empty(),
                "MooncakeBackend: group_id must not be empty");

    // Use user-provided tensor memory if available. Only its storage is used;
    // the Coordinator populates its contents through the core communicator.
    if (options_ && options_->activeRanks_.defined()) {
        activeRanks_ = options_->activeRanks_;
        TORCH_CHECK(activeRanks_.scalar_type() == at::kInt,
                    "active_ranks must have dtype int32");
        TORCH_CHECK(activeRanks_.is_contiguous(),
                    "active_ranks must be contiguous");
        TORCH_CHECK(activeRanks_.numel() >= max_group_size_,
                    "active_ranks is smaller than max_group_size");
        TORCH_CHECK(activeRanks_.is_cpu() || activeRanks_.is_cuda(),
                    "active_ranks must be on a CPU or supported GPU device");
    } else {
        activeRanks_ =
            at::empty({max_group_size_},
                      torch::dtype(torch::kInt32)
                          .device(isCpu_ ? torch::kCPU : torch::kCUDA));
    }
    // The mirror follows its tensor storage, independently of the communicator
    // device used for collectives.
    const bool active_ranks_mirror_is_device = !activeRanks_.is_cpu();

    std::vector<int32_t> global_ranks;
    if (distBackendOpts.global_ranks_in_group.empty()) {
        global_ranks.resize(size);
        std::iota(global_ranks.begin(), global_ranks.end(), 0);
    } else {
        TORCH_CHECK(distBackendOpts.global_ranks_in_group.size() ==
                        static_cast<size_t>(size),
                    "global_ranks_in_group must contain group_size entries");
        global_ranks.reserve(size);
        for (const auto global_rank : distBackendOpts.global_ranks_in_group) {
            TORCH_CHECK(global_rank >= 0 && global_rank < MOONCAKE_PG_MAX_RANKS,
                        "global rank is outside the supported range");
            global_ranks.push_back(static_cast<int32_t>(global_rank));
        }
    }

    mooncakePgCommConfig_t config = MOONCAKE_PG_COMM_CONFIG_INITIALIZER;

    // PyTorch's group_id is only a bootstrap id. The Coordinator resolves it
    // together with rank order into a process-lifetime GroupId. CPU and device
    // backends use independent namespaces.
    config.groupId = distBackendOpts.group_id.c_str();
    config.rank = rank;
    config.size = size;
    config.maxGroupSize = max_group_size_;
    config.globalRanks = global_ranks.data();
    config.globalRankCount = global_ranks.size();
    config.deviceIndex = isCpu_ ? -1 : at::cuda::current_device();
    config.deviceType = isCpu_ ? mooncakePgDeviceCpu : mooncakePgDeviceGpu;
    config.idResolvePolicy = options_ && options_->isExtension_
                                 ? mooncakePgIdResolveAttachOrExtend
                                 : mooncakePgIdResolveCreateOrAttach;
    config.autoDeactivateOnFailure =
        options_
            ? options_->autoDeactivateOnFailure_
            : c10::utils::check_env("MOONCAKE_PG_AUTO_DEACTIVATE_ON_FAILURE")
                  .value_or(true);
    config.autoSyncOnFailure =
        options_ ? options_->autoSyncOnFailure_
                 : c10::utils::check_env("MOONCAKE_PG_AUTO_SYNC_ON_FAILURE")
                       .value_or(true);
    // auto_sync_on_failure requires auto_deactivate_on_failure.
    TORCH_CHECK(!config.autoSyncOnFailure || config.autoDeactivateOnFailure,
                "auto_sync_on_failure requires "
                "auto_deactivate_on_failure=true");
    config.activeRanksMirror = activeRanks_.data_ptr<int32_t>();
    config.activeRanksMirrorCount = static_cast<size_t>(activeRanks_.numel());
    config.activeRanksMirrorIsDevice = active_ranks_mirror_is_device ? 1 : 0;
    config.activeRanksMirrorDeviceIndex =
        active_ranks_mirror_is_device ? activeRanks_.get_device() : -1;

    checkResult(mooncakePgCommCreate(context, &config, &comm_),
                "mooncakePgCommCreate");

    // Register a lightweight Backend shim so PyTorch dispatch can find a
    // registered Backend for this ProcessGroup. The shim delegates supported
    // P2P and collective operations back to this backend.
    const auto device_type =
        isCpu_ ? c10::DeviceType::CPU : c10::DeviceType::CUDA;
    auto shim = c10::make_intrusive<MooncakeBackendShim>(this, max_group_size_);
    setBackend(device_type, BackendType::CUSTOM, shim);
#ifndef MOONCAKE_EP_USE_MUSA
    setDefaultBackend(BackendType::CUSTOM);
#endif
}

MooncakeBackend::~MooncakeBackend() {
    try {
        shutdown();
    } catch (const std::exception& error) {
        TORCH_WARN("MooncakeBackend: shutdown failed during destruction: ",
                   error.what());
    } catch (...) {
        TORCH_WARN("MooncakeBackend: shutdown failed during destruction");
    }
}

const std::string MooncakeBackend::getBackendName() const { return "mooncake"; }

int MooncakeBackend::getSize() const {
    int size = 0;
    checkResult(mooncakePgCommGetSize(comm_, &size), "mooncakePgCommGetSize");
    return size;
}

template <auto CpuFn, auto GpuFn, typename... Args>
c10::intrusive_ptr<c10d::Work> MooncakeBackend::launchCollective(
    c10d::OpType opType, const char* operation, const at::Tensor& streamTensor,
    std::vector<at::Tensor> keepAlive, std::function<void()> postCompletion,
    Args... args) {
    auto failed_ranks_hint = FailedRanksHint::allocate(max_group_size_);
    const auto failed_ranks_hint_count = static_cast<size_t>(max_group_size_);
    if (isCpu_) {
        mooncakePgCompletion_t completion = nullptr;
        checkResult(CpuFn(args..., comm_, failed_ranks_hint.data(),
                          failed_ranks_hint_count, &completion),
                    operation);
        work_tracker_->evictCompleted();
        return c10::make_intrusive<MooncakeWorkCpu>(
            opType, completion, std::move(failed_ranks_hint), work_tracker_,
            std::move(keepAlive), std::move(postCompletion));
    }

    const auto stream = currentCudaStream(streamTensor);
    checkResult(GpuFn(args..., comm_, convertStream(stream),
                      failed_ranks_hint.data(), failed_ranks_hint_count),
                operation);
    if (postCompletion) postCompletion();
    auto event = std::make_shared<c10::Event>(c10::DeviceType::CUDA);
    event->record(stream);
    if (at::cuda::currentStreamCaptureStatus() ==
        c10::cuda::CaptureStatus::None) {
        work_tracker_->evictCompleted();
    }
    return c10::make_intrusive<MooncakeWorkCuda>(
        opType, std::move(event), std::move(failed_ranks_hint), work_tracker_,
        std::move(keepAlive));
}

template <auto CpuFn, auto GpuFn, typename... Args>
c10::intrusive_ptr<c10d::Work> MooncakeBackend::launchP2P(
    c10d::OpType opType, const char* operation, const at::Tensor& streamTensor,
    std::vector<at::Tensor> keepAlive, std::function<void()> postCompletion,
    Args... args) {
    auto failed_ranks_hint = FailedRanksHint::allocate(max_group_size_);
    const auto failed_ranks_hint_count = static_cast<size_t>(max_group_size_);
    mooncakePgCompletion_t completion = nullptr;
    if (isCpu_) {
        checkResult(CpuFn(args..., comm_, failed_ranks_hint.data(),
                          failed_ranks_hint_count, &completion),
                    operation);
    } else {
        const auto stream = currentCudaStream(streamTensor);
        checkResult(GpuFn(args..., comm_, convertStream(stream),
                          failed_ranks_hint.data(), failed_ranks_hint_count,
                          &completion),
                    operation);
    }

    if (isCpu_ || at::cuda::currentStreamCaptureStatus() ==
                      c10::cuda::CaptureStatus::None) {
        work_tracker_->evictCompleted();
    }
    return c10::make_intrusive<MooncakeP2PWork>(
        opType, completion, std::move(failed_ranks_hint), work_tracker_,
        std::move(keepAlive), std::move(postCompletion));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::send(
    std::vector<at::Tensor>& tensors, int dstRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, kSingleTensorError);
    auto tensor = tensors.back().contiguous();
    return launchP2P<mooncakePgSendCpu, mooncakePgSendGpu>(
        c10d::OpType::SEND, "mooncakePgSend", tensor, {tensor}, {},
        tensor.data_ptr(), tensorCount(tensor), tensorType(tensor), dstRank);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    (void)tag;
    TORCH_CHECK(tensors.size() == 1, kSingleTensorError);
    auto output = tensors.back();
    const bool copy_back = !output.is_contiguous();
    auto target = copy_back ? output.contiguous() : output;
    std::function<void()> post_completion;
    if (copy_back) {
        post_completion = [output, target, is_cpu = isCpu_]() mutable {
            output.copy_(target);
            if (!is_cpu) currentCudaStream(output).synchronize();
        };
    }
    return launchP2P<mooncakePgRecvCpu, mooncakePgRecvGpu>(
        c10d::OpType::RECV, "mooncakePgRecv", target, {output, target},
        std::move(post_completion), target.data_ptr(), tensorCount(target),
        tensorType(target), srcRank);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::broadcast(
    std::vector<at::Tensor>& tensors, const c10d::BroadcastOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, kSingleTensorError);
    auto tensor = tensors.back();
    const int root = opts.rootRank + opts.rootTensor;
    return launchCollective<mooncakePgBroadcastCpu, mooncakePgBroadcastGpu>(
        c10d::OpType::BROADCAST, "mooncakePgBroadcast", tensor, {tensor}, {},
        tensor.data_ptr(), tensor.data_ptr(), tensorCount(tensor),
        tensorType(tensor), root);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allreduce(
    std::vector<at::Tensor>& tensors, const c10d::AllreduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, kSingleTensorError);
    TORCH_CHECK(opts.sparseIndices == std::nullopt, kSparseError);
    auto tensor = tensors.back();
    return launchCollective<mooncakePgAllReduceCpu, mooncakePgAllReduceGpu>(
        c10d::OpType::ALLREDUCE, "mooncakePgAllReduce", tensor, {tensor}, {},
        tensor.data_ptr(), tensor.data_ptr(), tensorCount(tensor),
        tensorType(tensor), convertReduceOp(opts.reduceOp));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::allgather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllgatherOptions&) {
    TORCH_CHECK(inputTensors.size() == 1, kSingleTensorError);
    TORCH_CHECK(outputTensors.size() == 1, kSingleTensorError);
    auto input = inputTensors.back();
    auto outputs = outputTensors.back();
    const int active_size = getSize();
    validateEqualPeerTensors(outputs, input, active_size);
    auto packed_output =
        at::empty({input.numel() * static_cast<int64_t>(outputs.size())},
                  input.options());

    std::vector<at::Tensor> keep_alive{input, packed_output};
    keep_alive.insert(keep_alive.end(), outputs.begin(), outputs.end());
    auto post_completion =
        makeCopyBackToPeerTensors(packed_output, std::move(outputs));
    return launchCollective<mooncakePgAllGatherCpu, mooncakePgAllGatherGpu>(
        c10d::OpType::ALLGATHER, "mooncakePgAllGather", input,
        std::move(keep_alive), std::move(post_completion), input.data_ptr(),
        packed_output.data_ptr(), tensorCount(input), tensorType(input));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_allgather_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::AllgatherOptions&) {
    validateSingleBufferTensors(
        outputBuffer, inputBuffer,
        isCpu_ ? c10::DeviceType::CPU : c10::DeviceType::CUDA);

    return launchCollective<mooncakePgAllGatherCpu, mooncakePgAllGatherGpu>(
        c10d::OpType::_ALLGATHER_BASE, "mooncakePgAllGather", inputBuffer,
        {inputBuffer, outputBuffer}, {}, inputBuffer.data_ptr(),
        outputBuffer.data_ptr(), tensorCount(inputBuffer),
        tensorType(inputBuffer));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::_reduce_scatter_base(
    at::Tensor& outputBuffer, at::Tensor& inputBuffer,
    const c10d::ReduceScatterOptions& opts) {
    validateSingleBufferTensors(
        outputBuffer, inputBuffer,
        isCpu_ ? c10::DeviceType::CPU : c10::DeviceType::CUDA);

    return launchCollective<mooncakePgReduceScatterCpu,
                            mooncakePgReduceScatterGpu>(
        c10d::OpType::_REDUCE_SCATTER_BASE, "mooncakePgReduceScatter",
        outputBuffer, {inputBuffer, outputBuffer}, {}, inputBuffer.data_ptr(),
        outputBuffer.data_ptr(), tensorCount(outputBuffer),
        tensorType(outputBuffer), convertReduceOp(opts.reduceOp));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::alltoall(
    std::vector<at::Tensor>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::AllToAllOptions&) {
    TORCH_CHECK(!inputTensors.empty() && !outputTensors.empty(),
                "alltoall requires non-empty Tensor lists");
    const auto reference = inputTensors.front();
    const int active_size = getSize();
    validateEqualPeerTensors(outputTensors, reference, active_size);
    auto packed_input = packPeerTensors(inputTensors, reference, active_size);
    auto packed_output = at::empty(
        {reference.numel() * static_cast<int64_t>(outputTensors.size())},
        reference.options());

    std::vector<at::Tensor> keep_alive{packed_input, packed_output};
    keep_alive.insert(keep_alive.end(), inputTensors.begin(),
                      inputTensors.end());
    keep_alive.insert(keep_alive.end(), outputTensors.begin(),
                      outputTensors.end());
    auto post_completion =
        makeCopyBackToPeerTensors(packed_output, outputTensors);
    return launchCollective<mooncakePgAllToAllCpu, mooncakePgAllToAllGpu>(
        c10d::OpType::ALLTOALL, "mooncakePgAllToAll", reference,
        std::move(keep_alive), std::move(post_completion),
        packed_input.data_ptr(), packed_output.data_ptr(),
        tensorCount(reference), tensorType(reference));
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::barrier(
    const c10d::BarrierOptions&) {
    auto failed_ranks_hint = FailedRanksHint::allocate(max_group_size_);
    const auto failed_ranks_hint_count = static_cast<size_t>(max_group_size_);
    if (isCpu_) {
        mooncakePgCompletion_t completion = nullptr;
        checkResult(mooncakePgBarrierCpu(comm_, failed_ranks_hint.data(),
                                         failed_ranks_hint_count, &completion),
                    "mooncakePgBarrierCpu");
        work_tracker_->evictCompleted();
        return c10::make_intrusive<MooncakeWorkCpu>(
            c10d::OpType::BARRIER, completion, std::move(failed_ranks_hint),
            work_tracker_);
    }

    const auto stream = c10::cuda::getCurrentCUDAStream();
    checkResult(
        mooncakePgBarrierGpu(comm_, convertStream(stream),
                             failed_ranks_hint.data(), failed_ranks_hint_count),
        "mooncakePgBarrierGpu");
    auto event = std::make_shared<c10::Event>(c10::DeviceType::CUDA);
    event->record(stream);
    if (at::cuda::currentStreamCaptureStatus() ==
        c10::cuda::CaptureStatus::None) {
        work_tracker_->evictCompleted();
    }
    return c10::make_intrusive<MooncakeBarrierWorkCuda>(
        c10d::OpType::BARRIER, std::move(event), std::move(failed_ranks_hint),
        work_tracker_);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::reduce(
    std::vector<at::Tensor>& tensors, const c10d::ReduceOptions& opts) {
    TORCH_CHECK(tensors.size() == 1, kSingleTensorError);
    auto tensor = tensors.back();
    const int root = opts.rootRank + opts.rootTensor;
    return launchCollective<mooncakePgReduceCpu, mooncakePgReduceGpu>(
        c10d::OpType::REDUCE, "mooncakePgReduce", tensor, {tensor}, {},
        tensor.data_ptr(), tensor.data_ptr(), tensorCount(tensor),
        tensorType(tensor), convertReduceOp(opts.reduceOp), root);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::gather(
    std::vector<std::vector<at::Tensor>>& outputTensors,
    std::vector<at::Tensor>& inputTensors, const c10d::GatherOptions& opts) {
    TORCH_CHECK(inputTensors.size() == 1, kSingleTensorError);
    const int root = opts.rootRank;
    const bool is_root = root == rank_;
    if (is_root) {
        TORCH_CHECK(outputTensors.size() == 1, kSingleTensorError);
    }
    auto input = inputTensors.back();
    std::vector<at::Tensor> outputs;
    at::Tensor packed_output;
    if (is_root) {
        outputs = outputTensors.back();
        const int active_size = getSize();
        validateEqualPeerTensors(outputs, input, active_size);
        packed_output =
            at::empty({input.numel() * static_cast<int64_t>(outputs.size())},
                      input.options());
    }

    std::vector<at::Tensor> keep_alive{input, packed_output};
    keep_alive.insert(keep_alive.end(), outputs.begin(), outputs.end());
    auto post_completion = packed_output.defined() ? makeCopyBackToPeerTensors(
                                                         packed_output, outputs)
                                                   : std::function<void()>{};
    return launchCollective<mooncakePgGatherCpu, mooncakePgGatherGpu>(
        c10d::OpType::GATHER, "mooncakePgGather", input, std::move(keep_alive),
        std::move(post_completion), input.data_ptr(),
        packed_output.defined() ? packed_output.data_ptr() : nullptr,
        tensorCount(input), tensorType(input), root);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::scatter(
    std::vector<at::Tensor>& outputTensors,
    std::vector<std::vector<at::Tensor>>& inputTensors,
    const c10d::ScatterOptions& opts) {
    TORCH_CHECK(outputTensors.size() == 1, kSingleTensorError);
    const int root = opts.rootRank;
    const bool is_root = root == rank_;
    if (is_root) {
        TORCH_CHECK(inputTensors.size() == 1, kSingleTensorError);
    }
    auto output = outputTensors.back();
    at::Tensor packed_input;
    std::vector<at::Tensor> inputs;
    if (is_root) {
        inputs = inputTensors.back();
        packed_input = packPeerTensors(inputs, output, getSize());
    }

    std::vector<at::Tensor> keep_alive{output, packed_input};
    keep_alive.insert(keep_alive.end(), inputs.begin(), inputs.end());
    return launchCollective<mooncakePgScatterCpu, mooncakePgScatterGpu>(
        c10d::OpType::SCATTER, "mooncakePgScatter", output,
        std::move(keep_alive), {},
        packed_input.defined() ? packed_input.data_ptr() : nullptr,
        output.data_ptr(), tensorCount(output), tensorType(output), root);
}

void MooncakeBackend::shutdown() {
    if (isShutdown_) return;
    isShutdown_ = true;
    auto comm = std::exchange(comm_, nullptr);
    const auto result = comm ? mooncakePgCommDestroy(comm) : mooncakePgSuccess;
    work_tracker_->shutdown();
    checkResult(result, "mooncakePgCommDestroy");
}

void MooncakeBackend::extendGroupSizeTo(int) {
    // Deprecated: in the Coordinator-based path, group size is determined by
    // GroupView.rank_order. This is a no-op stub kept for compatibility.
    TORCH_WARN(
        "MooncakeBackend::extendGroupSizeTo is deprecated; group size "
        "is controlled by the Coordinator's GroupView.");
}

int MooncakeBackend::getNumSyncedRanks() {
    int num_synced_ranks = 0;
    checkResult(mooncakePgCommGetNumSyncedRanks(comm_, &num_synced_ranks),
                "mooncakePgCommGetNumSyncedRanks");
    return num_synced_ranks;
}

std::vector<bool> MooncakeBackend::getPeerState(const std::vector<int>& ranks) {
    const auto core_ranks = convertRanks(ranks);
    std::vector<int32_t> core_states(ranks.size(), 0);
    checkResult(
        mooncakePgCommGetPeerState(comm_, core_ranks.data(), core_ranks.size(),
                                   core_states.data()),
        "mooncakePgCommGetPeerState");
    std::vector<bool> result;
    result.reserve(core_states.size());
    for (const int32_t state : core_states) result.push_back(state != 0);
    return result;
}

mooncakePgProposalResponse_t MooncakeBackend::activateRanks(
    const std::vector<int>& ranks) {
    const auto core_ranks = convertRanks(ranks);
    mooncakePgProposalResponse_t response{};
    checkResult(mooncakePgCommActivateRanks(comm_, core_ranks.data(),
                                            core_ranks.size(), &response),
                "mooncakePgCommActivateRanks");
    return response;
}

mooncakePgProposalResponse_t MooncakeBackend::deactivateRanks(
    const std::vector<int>& ranks) {
    const auto core_ranks = convertRanks(ranks);
    mooncakePgProposalResponse_t response{};
    checkResult(mooncakePgCommDeactivateRanks(comm_, core_ranks.data(),
                                              core_ranks.size(), &response),
                "mooncakePgCommDeactivateRanks");
    return response;
}

void MooncakeBackend::joinGroup() {
    checkResult(mooncakePgCommJoin(comm_), "mooncakePgCommJoin");
}

uint64_t MooncakeBackend::getCurrentEpoch() const {
    uint64_t epoch = 0;
    checkResult(mooncakePgCommGetEpoch(comm_, &epoch),
                "mooncakePgCommGetEpoch");
    return epoch;
}

mooncakePgSyncAfterFailureResponse_t MooncakeBackend::syncAfterFailure() {
    mooncakePgSyncAfterFailureResponse_t response{};
    checkResult(mooncakePgCommSyncAfterFailure(comm_, &response),
                "mooncakePgCommSyncAfterFailure");
    return response;
}

}  // namespace mooncake
