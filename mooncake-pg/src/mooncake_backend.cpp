#include <ATen/cuda/CUDAContext.h>
#include <cuda.h>
#include <cuda_alike.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <p2p_proxy.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <atomic>
#include <sstream>
#include "connection_poller.h"
#include "memory_location.h"
#include "mooncake_pg_experimental.h"
#include "mooncake_worker.cuh"
#include "pg_utils.h"
#include <transport/device/device_transport.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mooncake {
namespace {

#ifdef USE_MACA
static void requireMacaHostTransport() {
    TORCH_CHECK(std::getenv("MC_MACA_HOST_TRANSPORT") != nullptr,
                "MACA PG requires MC_MACA_HOST_TRANSPORT=1 so the transfer "
                "engine uses a host transport.");
}

#endif

}  // namespace

constexpr const char* REGISTER_BUFFER_ERROR_MSG =
    "Failed to register local memory.";
constexpr const char* MULTI_DEVICE_ERROR_MSG =
    "Expecting one tensor only but got multiple.";
constexpr const char* SYNC_OP_ERROR_MSG = "Expecting async op but got sync op.";
constexpr const char* REDUCE_OP_ERROR_MSG = "Only support SUM.";
constexpr const char* SPARSE_ERROR_MSG = "Sparse op not supported.";
constexpr const char* REDUCE_DTYPE_ERROR_MSG = "Unsupported reduce dtype: ";
constexpr int kBarrierDummyTensorSize = 1;

static bool useDeviceControlRegionsForNvlink() {
#ifndef MOONCAKE_EP_USE_MUSA
    const char* mixed = std::getenv("MC_PG_NVLINK_MIXED");
    if (mixed && mixed[0] != '\0' && mixed[0] != '0') return false;
    const char* value = std::getenv("MC_INTRANODE_NVLINK");
    return value && value[0] != '\0' && value[0] != '0';
#else
    return false;
#endif
}

static bool useStoreSyncForNvlinkPoc() {
    const char* value = std::getenv("MOONCAKE_PG_INTRA_STORE_SYNC");
    return value && value[0] != '\0' && value[0] != '0';
}

static void maybeLogReplayTraceSequenceSlots(
    const char* op, int rank, int backendIndex, size_t bytes, uint32_t seqHint,
    const uint32_t* rsSequenceSlot, const uint32_t* agSequenceSlot, int slots,
    cudaStream_t stream) {
    if (!replayTraceSequenceEnabled()) return;

    cudaStreamCaptureStatus capture_status = cudaStreamCaptureStatusNone;
    cudaError_t capture_err = cudaStreamIsCapturing(stream, &capture_status);
    TORCH_CHECK(capture_err == cudaSuccess,
                "cudaStreamIsCapturing failed in Mooncake PG sequence trace: ",
                cudaGetErrorString(capture_err));
    if (capture_status != cudaStreamCaptureStatusNone) {
        LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE_SEQUENCE_SKIP op=" << op
                  << " rank=" << rank << " backend_index=" << backendIndex
                  << " bytes=" << bytes << " seq_hint=" << seqHint
                  << " capturing=" << static_cast<int>(capture_status);
        return;
    }

    cudaError_t sync_err = cudaStreamSynchronize(stream);
    TORCH_CHECK(sync_err == cudaSuccess,
                "Mooncake PG sequence trace sync failed for ", op, ": ",
                cudaGetErrorString(sync_err));

    uint32_t rs_sequence = 0;
    uint32_t ag_sequence = 0;
    if (rsSequenceSlot != nullptr) {
        cudaError_t copy_err = cudaMemcpy(&rs_sequence, rsSequenceSlot,
                                          sizeof(rs_sequence),
                                          cudaMemcpyDeviceToHost);
        TORCH_CHECK(copy_err == cudaSuccess,
                    "Mooncake PG sequence trace RS copy failed for ", op, ": ",
                    cudaGetErrorString(copy_err));
    }
    if (agSequenceSlot != nullptr) {
        cudaError_t copy_err = cudaMemcpy(&ag_sequence, agSequenceSlot,
                                          sizeof(ag_sequence),
                                          cudaMemcpyDeviceToHost);
        TORCH_CHECK(copy_err == cudaSuccess,
                    "Mooncake PG sequence trace AG copy failed for ", op, ": ",
                    cudaGetErrorString(copy_err));
    }

    const int rs_slot = slots > 0
        ? static_cast<int>(rs_sequence % static_cast<uint32_t>(slots))
        : -1;
    const int ag_slot = slots > 0
        ? static_cast<int>(ag_sequence % static_cast<uint32_t>(slots))
        : -1;
    LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE_SEQUENCE op=" << op
              << " rank=" << rank << " backend_index=" << backendIndex
              << " bytes=" << bytes << " seq_hint=" << seqHint
              << " slots=" << slots << " rs_seq=" << rs_sequence
              << " rs_slot=" << rs_slot << " ag_seq=" << ag_sequence
              << " ag_slot=" << ag_slot;
}

static bool syncAfterDeviceApiCollectiveTraceEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_SYNC_AFTER_DEVICE_API_COLLECTIVE");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool drainBeforeDeviceApiCollectiveEnabled() {
    const char* value =
        std::getenv("MOONCAKE_PG_DRAIN_BEFORE_DEVICE_API_COLLECTIVE");
    return value && value[0] != '\0' && value[0] != '0';
}

static void maybeSyncAfterDeviceApiCollective(const char* op, int rank,
                                              size_t bytes, uint32_t sequence,
                                              bool chunked, size_t chunks,
                                              cudaStream_t stream) {
    if (!syncAfterDeviceApiCollectiveTraceEnabled()) return;

    cudaStreamCaptureStatus capture_status = cudaStreamCaptureStatusNone;
    cudaError_t capture_err = cudaStreamIsCapturing(stream, &capture_status);
    TORCH_CHECK(capture_err == cudaSuccess,
                "cudaStreamIsCapturing failed in Mooncake PG diagnostic sync: ",
                cudaGetErrorString(capture_err));

    LOG(INFO) << "MOONCAKE_PG_DEVICE_API_SYNC_BEGIN op=" << op
              << " rank=" << rank << " bytes=" << bytes
              << " seq=" << sequence << " chunked=" << chunked
              << " chunks=" << chunks
              << " capturing=" << static_cast<int>(capture_status);

    if (capture_status != cudaStreamCaptureStatusNone) {
        LOG(INFO) << "MOONCAKE_PG_DEVICE_API_SYNC_SKIP_CAPTURE op=" << op
                  << " rank=" << rank << " bytes=" << bytes
                  << " seq=" << sequence;
        return;
    }

    cudaError_t sync_err = cudaStreamSynchronize(stream);
    TORCH_CHECK(sync_err == cudaSuccess,
                "Mooncake PG Device API collective diagnostic sync failed for ",
                op, ": ", cudaGetErrorString(sync_err));

    LOG(INFO) << "MOONCAKE_PG_DEVICE_API_SYNC_DONE op=" << op
              << " rank=" << rank << " bytes=" << bytes
              << " seq=" << sequence << " chunked=" << chunked
              << " chunks=" << chunks;
}

static void maybeDrainBeforeDeviceApiCollective(
    const MooncakeWorker* worker, const TransferGroupMeta* meta, const char* op,
    int rank, size_t bytes, uint32_t sequence, cudaStream_t stream) {
    if (!drainBeforeDeviceApiCollectiveEnabled()) return;

    cudaStreamCaptureStatus capture_status = cudaStreamCaptureStatusNone;
    cudaError_t capture_err = cudaStreamIsCapturing(stream, &capture_status);
    TORCH_CHECK(capture_err == cudaSuccess,
                "cudaStreamIsCapturing failed in Mooncake PG diagnostic drain: ",
                cudaGetErrorString(capture_err));

    LOG(INFO) << "MOONCAKE_PG_DEVICE_API_DRAIN_BEGIN op=" << op
              << " rank=" << rank << " backend_index=" << meta->backendIndex
              << " bytes=" << bytes << " seq=" << sequence
              << " stream=" << stream
              << " capturing=" << static_cast<int>(capture_status);
    const bool drained = worker->drainTasks(meta);
    LOG(INFO) << "MOONCAKE_PG_DEVICE_API_DRAIN_"
              << (drained ? "DONE" : "FAILED") << " op=" << op
              << " rank=" << rank << " backend_index=" << meta->backendIndex
              << " bytes=" << bytes << " seq=" << sequence
              << " stream=" << stream
              << " capturing=" << static_cast<int>(capture_status);
    TORCH_CHECK(drained,
                "Mooncake PG Device API diagnostic drain timed out before ", op,
                " rank=", rank, " backend_index=", meta->backendIndex,
                " bytes=", bytes, " seq=", sequence);
}

static bool forceDisableDirectP2pPeers() {
    const char* value = std::getenv("MOONCAKE_PG_FORCE_DISABLE_P2P");
    return value && value[0] != '\0' && value[0] != '0';
}

void MooncakeBackend::ensureCudaDevice() const {
    if (isCpu_ || cudaDeviceIndex_ < 0) return;
    cudaError_t err = cudaSetDevice(cudaDeviceIndex_);
    TORCH_CHECK(err == cudaSuccess,
                "Failed to restore Mooncake PG CUDA device ",
                cudaDeviceIndex_, ": ", cudaGetErrorString(err));
}

static std::string serverHostOnly(const std::string& server_name) {
    const auto pos = server_name.rfind(':');
    if (pos == std::string::npos) return server_name;
    return server_name.substr(0, pos);
}

static bool isCudaStreamCapturing(cudaStream_t stream) {
#ifndef __MUSA__
    cudaStreamCaptureStatus status = cudaStreamCaptureStatusNone;
    cudaError_t err = cudaStreamIsCapturing(stream, &status);
    if (err != cudaSuccess) return false;
    return status != cudaStreamCaptureStatusNone;
#else
    (void)stream;
    return false;
#endif
}

static uint64_t directP2pCountInterval() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_COUNT_INTERVAL");
    if (!value || value[0] == '\0') return 100;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return 100;
    return static_cast<uint64_t>(parsed);
}

static bool shouldLogDirectP2pCount(std::atomic<uint64_t>& counter) {
    if (!directP2pCountEnabled()) return false;
    const uint64_t value = counter.fetch_add(1, std::memory_order_relaxed) + 1;
    return value <= 8 || value % directP2pCountInterval() == 0;
}

static std::atomic<uint64_t> g_directP2pAgFastCount{0};
static std::atomic<uint64_t> g_directP2pAgFallbackCount{0};
static std::atomic<uint64_t> g_directP2pRsFastCount{0};
static std::atomic<uint64_t> g_directP2pRsFallbackCount{0};
static std::atomic<uint64_t> g_directP2pArFastCount{0};
static std::atomic<uint64_t> g_directP2pArFallbackCount{0};

static int deviceRingAllgatherChannels() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG_CHANNELS");
    if (!value || value[0] == '\0') return 1;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed < 1) return 1;
    if (parsed > 32) return 32;
    return static_cast<int>(parsed);
}

static int directP2pAllReduceRingChannels() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AR_RING_CHANNELS");
    if (!value || value[0] == '\0') return 32;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed < 1) return 1;
    if (parsed > 32) return 32;
    return static_cast<int>(parsed);
}

static int deviceRingAllgatherFifoSlots(int numRanks) {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG_FIFO_SLOTS");
    long parsed = numRanks > 1 ? numRanks - 1 : 1;
    if (value && value[0] != '\0') {
        char* end = nullptr;
        long env_parsed = std::strtol(value, &end, 10);
        if (end != value && env_parsed >= 1) parsed = env_parsed;
    }
    if (parsed < 1) parsed = 1;
    if (parsed > 16) parsed = 16;
    return static_cast<int>(parsed);
}

static size_t directP2pSignalBytesFor(int channels) {
    return 2 * static_cast<size_t>(channels) * kMaxNumRanks * sizeof(uint32_t);
}

static size_t directP2pFifoSignalBytesFor(int channels, int fifoSlots) {
    return 2 * static_cast<size_t>(channels) * static_cast<size_t>(fifoSlots) *
           sizeof(uint32_t);
}

static size_t directP2pFifoBytesFor(size_t tensorSize, int channels,
                                    int fifoSlots) {
    const size_t channelStride =
        (tensorSize + static_cast<size_t>(channels) - 1) /
        static_cast<size_t>(channels);
    return static_cast<size_t>(channels) * static_cast<size_t>(fifoSlots) *
           channelStride;
}

static inline void cpuSpinRelax() {
#if defined(__x86_64__) || defined(__i386__)
    asm volatile("pause" ::: "memory");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

static std::string directP2pAllgatherKey(int backendIndex, int rank) {
    return "mooncake_pg_direct_p2p_ag/" + std::to_string(backendIndex) + "/" +
           std::to_string(rank);
}

static std::string directP2pAllgatherOutputKey(int backendIndex, int sequence,
                                               int rank) {
    return "mooncake_pg_direct_p2p_ag_output/" +
           std::to_string(backendIndex) + "/" + std::to_string(sequence) +
           "/" + std::to_string(rank);
}

static uint64_t directP2pIpcHandleFingerprint(
    const cudaIpcMemHandle_t& handle) {
    const auto* handle_bytes = reinterpret_cast<const unsigned char*>(&handle);
    uint64_t hash = 1469598103934665603ull;
    for (size_t i = 0; i < sizeof(cudaIpcMemHandle_t); ++i) {
        hash ^= static_cast<uint64_t>(handle_bytes[i]);
        hash *= 1099511628211ull;
    }
    return hash;
}

static std::string directP2pAllgatherSyncKey(int backendIndex, int rank,
                                             uint32_t sequence) {
    return "mooncake_pg_direct_p2p_ag_sync/" +
           std::to_string(backendIndex) + "/" + std::to_string(sequence) +
           "/" + std::to_string(rank);
}

static std::string directP2pAllgatherShmNameKey(int backendIndex) {
    return "mooncake_pg_direct_p2p_ag_shm_name/" +
           std::to_string(backendIndex);
}

static std::string directP2pAllgatherShmReadyKey(int backendIndex) {
    return "mooncake_pg_direct_p2p_ag_shm_ready/" +
           std::to_string(backendIndex);
}

static std::string directRdmaMetadataKey(int backendIndex, int rank) {
    return "mooncake_pg_device_rdma/" + std::to_string(backendIndex) + "/" +
           std::to_string(rank);
}

static std::string hierarchicalAllReduceP2pKey(int backendIndex, int rank) {
    return "mooncake_pg_device_hier_ar_p2p/" +
           std::to_string(backendIndex) + "/" + std::to_string(rank);
}

static std::string hierarchicalAllReduceRdmaKey(int backendIndex, int rank) {
    return "mooncake_pg_device_hier_ar_rdma/" +
           std::to_string(backendIndex) + "/" + std::to_string(rank);
}

static std::vector<std::string> parseDeviceFilterEnv() {
    const char* raw = std::getenv("MOONCAKE_PGTEST_DEVICE_FILTERS");
    std::vector<std::string> filters;
    if (!raw || raw[0] == '\0') return filters;
    std::stringstream ss(raw);
    std::string item;
    while (std::getline(ss, item, ',')) {
        const auto begin = item.find_first_not_of(" \t\n\r");
        if (begin == std::string::npos) continue;
        const auto end = item.find_last_not_of(" \t\n\r");
        filters.push_back(item.substr(begin, end - begin + 1));
    }
    return filters;
}

template <typename T>
static void appendRdmaBytes(std::vector<uint8_t>& out, const T& value) {
    const auto* ptr = reinterpret_cast<const uint8_t*>(&value);
    out.insert(out.end(), ptr, ptr + sizeof(T));
}

template <typename T>
static T readRdmaBytes(const std::vector<uint8_t>& in, size_t& offset) {
    TORCH_CHECK(offset + sizeof(T) <= in.size(),
                "Invalid Mooncake PG Device API RDMA metadata.");
    T value;
    std::memcpy(&value, in.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}

static std::vector<uint8_t> serializeRdmaMetadata(
    const device::RdmaLocalMetadata& meta) {
    std::vector<uint8_t> out;
    appendRdmaBytes(out, meta.raddr);
    appendRdmaBytes(out, meta.rkey);
    appendRdmaBytes(out, meta.subnet_prefix);
    appendRdmaBytes(out, meta.interface_id);
    const uint32_t qp_count = static_cast<uint32_t>(meta.qpns.size());
    const uint32_t lid_count = static_cast<uint32_t>(meta.lids.size());
    appendRdmaBytes(out, qp_count);
    appendRdmaBytes(out, lid_count);
    for (int32_t qpn : meta.qpns) appendRdmaBytes(out, qpn);
    for (int32_t lid : meta.lids) appendRdmaBytes(out, lid);
    return out;
}

static device::RdmaLocalMetadata deserializeRdmaMetadata(
    const std::vector<uint8_t>& bytes) {
    size_t offset = 0;
    device::RdmaLocalMetadata meta;
    meta.raddr = readRdmaBytes<int64_t>(bytes, offset);
    meta.rkey = readRdmaBytes<int32_t>(bytes, offset);
    meta.subnet_prefix = readRdmaBytes<int64_t>(bytes, offset);
    meta.interface_id = readRdmaBytes<int64_t>(bytes, offset);
    const uint32_t qp_count = readRdmaBytes<uint32_t>(bytes, offset);
    const uint32_t lid_count = readRdmaBytes<uint32_t>(bytes, offset);
    meta.qpns.reserve(qp_count);
    meta.lids.reserve(lid_count);
    for (uint32_t i = 0; i < qp_count; ++i) {
        meta.qpns.push_back(readRdmaBytes<int32_t>(bytes, offset));
    }
    for (uint32_t i = 0; i < lid_count; ++i) {
        meta.lids.push_back(readRdmaBytes<int32_t>(bytes, offset));
    }
    TORCH_CHECK(offset == bytes.size(),
                "Unexpected trailing Mooncake PG Device API RDMA metadata.");
    return meta;
}

class MooncakeEventWorkCuda : public ::c10d::Work {
   public:
    MooncakeEventWorkCuda(c10d::OpType opType,
                          std::shared_ptr<torch::Event> event)
        : Work(-1, opType), event_(std::move(event)) {}

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override {
        (void)timeout;
        // Match CUDA ProcessGroup/NCCL-style semantics: for a synchronous
        // Python collective on CUDA, wait() should make the result visible to
        // the caller's current stream, not force a CPU-side cudaEventSynchronize
        // on every collective.  CPU synchronization here turns rank enqueue
        // skew into milliseconds of device-side peer wait for the direct P2P
        // kernels.
        event_->block(at::cuda::getCurrentCUDAStream());
        return true;
    }

   private:
    std::shared_ptr<torch::Event> event_;
};

class MooncakeAlreadyEnqueuedWorkCuda : public ::c10d::Work {
   public:
    explicit MooncakeAlreadyEnqueuedWorkCuda(c10d::OpType opType)
        : Work(-1, opType) {}

    bool isCompleted() override { return true; }

    bool wait(std::chrono::milliseconds timeout) override {
        (void)timeout;
        return true;
    }
};

std::string MooncakeBackend::hostIp_ = "127.0.0.1";
// leaky singleton to avoid destructor fiasco problem
TransferEngine* MooncakeBackend::engine_ = new TransferEngine(true);
std::vector<std::string> MooncakeBackend::deviceFilters_;
// worker_ is now owned per backend instance via MooncakeWorkerManager.
bool MooncakeBackend::engineInitialized_ = false;
int MooncakeBackend::backendIndex_ = 0;
TransferEngine* MooncakeBackend::externalEngine_ = nullptr;

std::vector<uint8_t> serialize(const ExtensionState& state) {
    uint32_t rankCount = static_cast<uint32_t>(state.activeRanks.size());

    // Calculate bytes needed for the bitmap: 1 bit per rank, rounded up to
    // nearest byte
    size_t bitmapSize = (rankCount + 7) / 8;

    // Total size = count field + bitmap + p2pEpochs[] + taskCount
    size_t totalSize = sizeof(uint32_t) + bitmapSize +
                       sizeof(uint32_t) * rankCount + sizeof(int32_t);

    std::vector<uint8_t> buffer(totalSize, 0);
    uint8_t* ptr = buffer.data();

    // 1. Store the number of ranks
    std::memcpy(ptr, &rankCount, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 2. Store activeRanks as a bitset
    for (size_t i = 0; i < rankCount; ++i) {
        if (state.activeRanks[i]) {
            // Set the i-th bit to 1 if the rank is active
            ptr[i / 8] |= (1 << (i % 8));
        }
    }
    ptr += bitmapSize;

    // 3. Store per-peer p2pEpochs
    for (size_t i = 0; i < rankCount; ++i) {
        std::memcpy(ptr + i * sizeof(uint32_t), &state.p2pEpochs[i],
                    sizeof(uint32_t));
    }
    ptr += sizeof(uint32_t) * rankCount;

    // 4. Store taskCount
    int32_t taskCount = static_cast<int32_t>(state.taskCount);
    std::memcpy(ptr, &taskCount, sizeof(int32_t));

    return buffer;
}

ExtensionState deserialize(const std::vector<uint8_t>& buffer) {
    ExtensionState state;
    if (buffer.size() < sizeof(uint32_t)) return state;

    const uint8_t* ptr = buffer.data();

    // 1. Read the number of ranks
    uint32_t rankCount = 0;
    std::memcpy(&rankCount, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Calculate expected total size and verify buffer is sufficient before
    // proceeding with further reads.
    size_t bitmapSize = (rankCount + 7) / 8;
    size_t expectedSize = sizeof(uint32_t) + bitmapSize +
                          sizeof(uint32_t) * rankCount + sizeof(int32_t);
    if (buffer.size() < expectedSize) return state;

    // 2. Read the bitmap and reconstruct the activeRanks vector
    state.activeRanks.resize(rankCount);
    for (size_t i = 0; i < rankCount; ++i) {
        // Check if the i-th bit is set
        bool isActive = ptr[i / 8] & (1 << (i % 8));
        state.activeRanks[i] = isActive;
    }
    ptr += bitmapSize;

    // 3. Read per-peer p2pEpochs
    state.p2pEpochs.resize(rankCount);
    for (size_t i = 0; i < rankCount; ++i) {
        std::memcpy(&state.p2pEpochs[i], ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
    }

    // 4. Read taskCount
    int32_t taskCount = 0;
    std::memcpy(&taskCount, ptr, sizeof(int32_t));
    state.taskCount = static_cast<int>(taskCount);

    return state;
}

// Async Work implementation for P2P operations processed by worker threads.
class MooncakeP2PWork : public ::c10d::Work {
   public:
    explicit MooncakeP2PWork(
        std::shared_ptr<std::atomic<P2PProxy::OpStatus>> status)
        : Work(-1, c10d::OpType::UNKNOWN), status_(status) {}

    bool isCompleted() override {
        return status_->load(std::memory_order_acquire) !=
               P2PProxy::OpStatus::kPending;
    }

    bool isSuccess() const override {
        return status_->load(std::memory_order_acquire) ==
               P2PProxy::OpStatus::kSuccess;
    }

    bool wait(std::chrono::milliseconds timeout) override {
        BackoffWaiterConfig cfg{};
        cfg.max_sleep = std::chrono::microseconds(10);
        BackoffWaiter waiter(cfg);

        bool done = false;
        if (timeout.count() > 0) {
            done = waiter.wait_for(timeout, [this] {
                return status_->load(std::memory_order_acquire) !=
                       P2PProxy::OpStatus::kPending;
            });
        } else {
            waiter.wait([this] {
                return status_->load(std::memory_order_acquire) !=
                       P2PProxy::OpStatus::kPending;
            });
            done = true;
        }

        if (!done) {
            return false;
        }

        if (status_->load(std::memory_order_acquire) ==
            P2PProxy::OpStatus::kFailed) {
            TORCH_CHECK(false, "Mooncake P2P operation failed.");
        }
        return true;
    }

   private:
    std::shared_ptr<std::atomic<P2PProxy::OpStatus>> status_;
};

/**
 * @brief Initialize Mooncake backend state from the PyTorch process-group
 * information and optional Mooncake-specific options.
 */
MooncakeBackend::MooncakeBackend(
    c10d::DistributedBackendOptions distBackendOpts,
    c10::intrusive_ptr<MooncakeBackendOptions> options, bool isCpu)
    : ProcessGroup(distBackendOpts.store, distBackendOpts.group_rank,
                   distBackendOpts.group_size),
      options_(std::move(options)),
      isCpu_(isCpu) {
    auto store = std::move(distBackendOpts.store);
    const int rank = distBackendOpts.group_rank;
    const int size = distBackendOpts.group_size;
    const int max_size = (options_ && options_->maxWorldSize_ > 0)
                             ? options_->maxWorldSize_
                             : size;

    TORCH_CHECK(max_size >= 0 && static_cast<size_t>(max_size) <= kMaxNumRanks,
                "max_world_size out of range");
    TORCH_CHECK(max_size >= size,
                "max_world_size must be >= process group size");
    const auto& globalRanks = distBackendOpts.global_ranks_in_group;

    // Memory location for device specific buffers
    // always kWildcardLocation for cpu backend
    std::string location = kWildcardLocation;
    if (!isCpu) {
        int deviceCount = 0;
        cudaError_t err = cudaGetDeviceCount(&deviceCount);
        if (err == cudaSuccess && deviceCount != 0) {
            int deviceId = -1;
            err = cudaGetDevice(&deviceId);
            TORCH_CHECK(!err, c10::str("Failed to get device id"));
            cudaDeviceIndex_ = deviceId;
            location = GPU_PREFIX + std::to_string(cudaDeviceIndex_);
            device_collective_runtime_.cuda_device_index = cudaDeviceIndex_;
        }
    }

    // Initialize transfer engine
    if (externalEngine_) {
        // Use externally-provided engine (already initialized), skip init.
        engine_ = externalEngine_;
        engineInitialized_ = true;
    } else if (!engineInitialized_) {
#ifdef USE_MACA
        requireMacaHostTransport();
#endif
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

    // Fill the remaining slots for polling / future joiners.
    for (int i = size; i < max_size; ++i) {
        local2global_rank_map_[i] = i;
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

#ifdef USE_MACA
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err,
                        c10::str("Failed to allocate MACA GPU send buffer"));

            int rc = engine_->registerLocalMemory(send_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err,
                        c10::str("Failed to allocate MACA GPU recv buffer"));

            int rc = engine_->registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
#else
    } else {
        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&send_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA send buffer"));

            int rc = engine_->registerLocalMemory(send_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }

        for (size_t i = 0; i < 2; i++) {
            cudaError_t err = cudaMalloc(&recv_buffer_[i], kBufferSize);
            TORCH_CHECK(!err, c10::str("Failed to allocate CUDA recv buffer"));

            int rc = engine_->registerLocalMemory(recv_buffer_[i], kBufferSize,
                                                  location);
            TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
        }
#endif
    }

    // Register sync regions.  The classic intra-node NVLink transport only
    // accepts cudaMemoryTypeDevice buffers, so keep the legacy CPU regions for
    // RDMA/TCP but move this tiny control plane to CUDA memory when that
    // transport is explicitly enabled.
    TORCH_CHECK(static_cast<size_t>(size) <= kMaxNumRanks,
                "The number of ranks exceeds the limit.");
    syncRegionsOnDevice_ = !isCpu && useDeviceControlRegionsForNvlink();
    for (size_t i = 0; i < 2; i++) {
        if (syncRegionsOnDevice_) {
            cudaError_t err = cudaMalloc(&cpu_sync_send_region_[i],
                                         kMaxNumRanks * sizeof(int32_t));
            TORCH_CHECK(err == cudaSuccess,
                        "Failed to allocate CUDA sync send region: ",
                        cudaGetErrorString(err));
            err = cudaMemset(cpu_sync_send_region_[i], 0,
                             kMaxNumRanks * sizeof(int32_t));
            TORCH_CHECK(err == cudaSuccess,
                        "Failed to clear CUDA sync send region: ",
                        cudaGetErrorString(err));
            int32_t one = 1;
            err = cudaMemcpy(cpu_sync_send_region_[i], &one, sizeof(one),
                             cudaMemcpyHostToDevice);
            TORCH_CHECK(err == cudaSuccess,
                        "Failed to initialize CUDA sync send region: ",
                        cudaGetErrorString(err));
        } else {
            cpu_sync_send_region_[i] = new int32_t[kMaxNumRanks]{};
        }
        int rc = engine_->registerLocalMemory(cpu_sync_send_region_[i],
                                              kMaxNumRanks * sizeof(int32_t),
                                              kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    for (size_t i = 0; i < 2; i++) {
        if (syncRegionsOnDevice_) {
            cudaError_t err = cudaMalloc(&cpu_sync_recv_region_[i],
                                         kMaxNumRanks * sizeof(int32_t));
            TORCH_CHECK(err == cudaSuccess,
                        "Failed to allocate CUDA sync recv region: ",
                        cudaGetErrorString(err));
            err = cudaMemset(cpu_sync_recv_region_[i], 0,
                             kMaxNumRanks * sizeof(int32_t));
            TORCH_CHECK(err == cudaSuccess,
                        "Failed to clear CUDA sync recv region: ",
                        cudaGetErrorString(err));
        } else {
            cpu_sync_recv_region_[i] = new int32_t[kMaxNumRanks]{};
        }
        int rc = engine_->registerLocalMemory(cpu_sync_recv_region_[i],
                                              kMaxNumRanks * sizeof(int32_t),
                                              kWildcardLocation);
        TORCH_CHECK(!rc, REGISTER_BUFFER_ERROR_MSG);
    }

    auto& dev_worker_mgr = P2PDeviceWorkerManager::getInstance();
    int cuda_device_index = isCpu_ ? -1 : at::cuda::current_device();
    if (!isCpu_) cudaDeviceIndex_ = cuda_device_index;
    if (!isCpu_ && directP2pAllgatherDebug()) {
        LOG(INFO) << "MOONCAKE_PG_BACKEND_CUDA_DEVICE rank=" << rank
                  << " size=" << size
                  << " saved_cuda_device=" << cudaDeviceIndex_
                  << " current_device=" << cuda_device_index;
        device_collective_runtime_.cuda_device_index = cuda_device_index;
    }

    if (isCpu_)
        p2p_device_worker_ = dev_worker_mgr.getCPUWorker(engine_);
    else
        p2p_device_worker_ =
            dev_worker_mgr.getCUDAWorker(cuda_device_index, engine_);

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
                     .size = max_size,
                     .cuda_device_index = cuda_device_index,
                 });
    p2p_device_worker_->registerProxy(p2p_proxy_);

    meta_ = std::make_shared<TransferGroupMeta>();
    connection_ctx_ = std::make_shared<ConnectionContext>(
        backendIndex_, rank, size, options_ && options_->isExtension_,
        local2global_rank_map_, store, meta_, p2p_proxy_, engine_);

    if (max_size != size) {
        connection_ctx_->setPollingLimitTo(max_size);
    }

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
    rank_info.p2p_credit_region = (uint64_t)p2p_proxy_->credit_region();
    rank_info.p2p_ack_region = (uint64_t)p2p_proxy_->ack_region();

    // Sync metadata
    std::vector<uint8_t> rank_info_bytes(sizeof(SegmentInfo));
    memcpy(rank_info_bytes.data(), &rank_info, sizeof(SegmentInfo));
    meta_->rank = rank;
    // NOTE: meta_->size is intentionally initialized to max_world_size (when
    // provided) so that healthy ranks can activate joiners via recoverRanks()
    // without calling extendGroupSizeTo(). Inactive slots are masked by
    // meta_->activeRanks / meta_->activeRanksTensor.
    meta_->size = max_size;
    // activeSize tracks the visible group size (returned by getSize() /
    // dist.get_world_size()). It starts at the actual member count and grows
    // when extendGroupSizeTo() or recoverRanks() expands the group.
    // For extension ranks, activeSize equals world_size (= max_world_size);
    // the local-only behavior before joinGroup() is ensured by activeRanks
    // masking, not by a smaller activeSize.
    meta_->activeSize = size;
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

    // Reserve extra slots as inactive so collectives won't wait on them.
    for (int i = size; i < max_size; ++i) {
        meta_->activeRanks[i] = false;
    }
    if (options_ && options_->activeRanks_.defined()) {
        TORCH_CHECK(options_->activeRanks_.dtype() == at::kInt,
                    "activeRanks must be int.");
        if (isCpu) {
            TORCH_CHECK(options_->activeRanks_.device().is_cpu(),
                        "activeRanks must be on CPU.");
        } else {
            TORCH_CHECK(
                options_->activeRanks_.device().type() == c10::DeviceType::CUDA,
                "activeRanks must be on GPU.");
        }
        if (max_size != size) {
            TORCH_CHECK(options_->activeRanks_.numel() == max_size,
                        "activeRanks must be sized to max_world_size when "
                        "max_world_size is set");
        }
        meta_->activeRanksTensor = options_->activeRanks_;
    } else {
        meta_->activeRanksTensor = at::ones(
            {max_size}, torch::dtype(torch::kInt32)
                            .device(isCpu ? torch::kCPU : torch::kCUDA));
        if (max_size != size) {
            meta_->activeRanksTensor.slice(0, size, max_size).fill_(0);
        }
    }
    meta_->engine = engine_;
    meta_->store = store;
    meta_->backendIndex = backendIndex_;
    meta_->bufferBaseIndex = backendIndex_ * 10;
    meta_->syncOnDevice = syncRegionsOnDevice_;
    meta_->storeSync = !isCpu && useStoreSyncForNvlinkPoc();
    p2p_proxy_->bindMeta(meta_);

    connection_ctx_->bootstrapLocalPeer(localServerName_, rank_info);
    if (options_ && options_->isExtension_) {
        setLocalOnlyActiveRanks();
    } else {
        publishLocalPeerMetadata();
        ConnectionPoller::GetInstance().registerContext(connection_ctx_);
        connectionPollerRegistered_ = true;
        connection_ctx_->waitUntilAllConnected();
        maybeInitDeviceCollectiveRuntime();
        maybeInitDirectP2pAllgather();
        maybeInitHierarchicalAllReduceWorkspace();
    }

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
    ++backendIndex_;
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
    connection_ctx_->waitUntilNewRanksConnected();

    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_->store, "P2P send requires a valid Store.");
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

    TORCH_CHECK(p2p_proxy_, "P2P send proxy is not initialized.");
    p2p_proxy_->enqueueSend(P2PProxy::SendOp{
        .tensor_ = std::move(contiguous),
        .peer_rank_ = dstRank,
        .cuda_stream_ = stream,
        .status_ = status,
    });

    return c10::make_intrusive<MooncakeP2PWork>(status);
}

c10::intrusive_ptr<c10d::Work> MooncakeBackend::recv(
    std::vector<at::Tensor>& tensors, int srcRank, int tag) {
    connection_ctx_->waitUntilNewRanksConnected();

    (void)tag;
    TORCH_CHECK(tensors.size() == 1, MULTI_DEVICE_ERROR_MSG);
    auto tensor = tensors.back();

    TORCH_CHECK(meta_->store, "P2P recv requires a valid Store.");
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

    TORCH_CHECK(p2p_proxy_, "P2P recv proxy is not initialized.");
    p2p_proxy_->enqueueRecv(P2PProxy::RecvOp{
        .tensor_ = target,
        .original_tensor_ = tensor,
        .peer_rank_ = srcRank,
        .cuda_stream_ = stream,
        .status_ = status,
    });

    return c10::make_intrusive<MooncakeP2PWork>(status);
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
        if (captureTraceEnabled()) {
            LOG(INFO) << "MOONCAKE_PG_CAPTURE_TRACE op=broadcast rank="
                      << rank_ << " active=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " dtype=" << tensor.scalar_type()
                      << " capturing=" << isCudaStreamCapturing(stream.stream())
                      << " root=" << root;
        }
        return worker_->putTaskCuda(
            c10d::OpType::BROADCAST, tensorSize, root, meta_, connection_ctx_,
            stream,
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
        const bool is_capturing = isCudaStreamCapturing(stream.stream());
        if (captureTraceEnabled()) {
            LOG(INFO) << "MOONCAKE_PG_CAPTURE_TRACE op=all_reduce rank="
                      << rank_ << " backend_index=" << meta_->backendIndex
                      << " active=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " dtype=" << tensor.scalar_type()
                      << " capturing=" << is_capturing
                      << " contiguous=" << tensor.is_contiguous()
                      << " reduce_op=" << static_cast<int>(opts.reduceOp);
        }
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            is_capturing;
        const int store_signal_slots = deviceStoreSignalSlots();
        const size_t direct_buffer_size = directP2pEffectiveBufferSize();
        const size_t direct_signal_bytes = directP2pSlottedControlBytes(
            store_signal_slots, useDeviceApiCollectivesPoc());
        const size_t direct_slot_stride = directP2pSlotStride(
            direct_buffer_size, direct_signal_bytes, store_signal_slots,
            meta_->activeSize);
        const bool use_device_sequence = useDirectP2pDeviceSequence() &&
            directP2pDeviceSequenceCounter_ && directP2pDeviceSequenceSlots_;
        const size_t direct_ar_segment_bytes =
            meta_->activeSize > 0
                ? tensorSize / static_cast<size_t>(meta_->activeSize)
                : 0;
        const size_t rdma_ar_slot_stride = directP2pSlotStride(
            direct_buffer_size, direct_signal_bytes, store_signal_slots,
            meta_->activeSize * 2);
        const size_t rdma_ar_send_base_offset =
            rdma_ar_slot_stride * static_cast<size_t>(store_signal_slots) *
            static_cast<size_t>(meta_->activeSize);
        const bool can_device_api_rdma_ar = !direct_p2p_capture_disabled &&
            useDeviceApiCollectivesPoc() && directRdmaReady_ &&
            useDirectP2pAllReducePoc() && useDirectP2pReduceScatterPoc() &&
            useDirectP2pAllgatherPoc() && use_device_sequence &&
            tensor.is_contiguous() && meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && meta_->activeSize > 0 &&
            tensorSize % static_cast<size_t>(meta_->activeSize) == 0 &&
            direct_ar_segment_bytes % tensor.element_size() == 0 &&
            rdma_ar_slot_stride > 0 &&
            (direct_ar_segment_bytes <= rdma_ar_slot_stride ||
             useDirectP2pChunked());
        const bool can_device_api_p2p_full_ar = false;
        const bool can_device_api_p2p_ar_rsag =
            !direct_p2p_capture_disabled &&
            useDeviceApiCollectivesPoc() &&
            useDirectP2pAllReducePoc() && useDirectP2pReduceScatterPoc() &&
            useDirectP2pAllgatherPoc() && useDirectP2pChunked() &&
            directP2pAllgatherReady_ && use_device_sequence &&
            tensor.is_contiguous() && meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && meta_->activeSize > 0 &&
            tensorSize % static_cast<size_t>(meta_->activeSize) == 0 &&
            direct_ar_segment_bytes % tensor.element_size() == 0 &&
            direct_slot_stride > 0 && allActivePeersOnSameHost() &&
            !forceDisableDirectP2pPeers();
        const bool device_api_ar_requested =
            useDeviceApiCollectivesPoc() && useDirectP2pAllReducePoc();
        if (device_api_ar_requested) {
            TORCH_CHECK(
                can_device_api_rdma_ar || can_device_api_p2p_full_ar ||
                    can_device_api_p2p_ar_rsag,
                "Mooncake PG Device API all_reduce requested but path is not "
                "ready: capture_disabled=", direct_p2p_capture_disabled,
                " rdma_ready=", directRdmaReady_,
                " p2p_ready=", directP2pAllgatherReady_,
                " direct_ar=", useDirectP2pAllReducePoc(),
                " direct_rs=", useDirectP2pReduceScatterPoc(),
                " direct_ag=", useDirectP2pAllgatherPoc(),
                " device_sequence=", use_device_sequence,
                " contiguous=", tensor.is_contiguous(),
                " full_active=", meta_->activeSize == meta_->size,
                " sum=", opts.reduceOp == c10d::ReduceOp::SUM,
                " same_host=", allActivePeersOnSameHost(),
                " active_size=", meta_->activeSize,
                " bytes=", tensorSize,
                " segment_bytes=", direct_ar_segment_bytes,
                " rdma_slot_stride=", rdma_ar_slot_stride,
                " full_ar=", can_device_api_p2p_full_ar,
                " rsag=", can_device_api_p2p_ar_rsag);
        }
        const bool can_direct_ar_rsag = !direct_p2p_capture_disabled &&
            useDirectP2pAllReducePoc() && useDirectP2pReduceScatterPoc() &&
            useDirectP2pAllgatherPoc() && useDirectP2pChunked() &&
            directP2pAllgatherReady_ && tensor.is_contiguous() &&
            meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && meta_->activeSize > 0 &&
            tensorSize % static_cast<size_t>(meta_->activeSize) == 0 &&
            direct_ar_segment_bytes % tensor.element_size() == 0 &&
            direct_slot_stride > 0 &&
            (!useDeviceApiCollectivesPoc() ||
             (use_device_sequence && allActivePeersOnSameHost() &&
              !forceDisableDirectP2pPeers()));
        int cuda_device_count = 0;
        cudaGetDeviceCount(&cuda_device_count);
        const size_t hier_signal_bytes =
            static_cast<size_t>(store_signal_slots) * kMaxNumRanks *
            sizeof(uint32_t);
        size_t hier_slot_stride = 0;
        if (hierarchicalAllReduceWorkspaceReady_ && cuda_device_count > 0 &&
            hierarchicalAllReduceBufferBytes_ > 5 * hier_signal_bytes) {
            hier_slot_stride =
                ((hierarchicalAllReduceBufferBytes_ - 5 * hier_signal_bytes) /
                 static_cast<size_t>(store_signal_slots) /
                 static_cast<size_t>(cuda_device_count + 2)) &
                ~static_cast<size_t>(7);
        }
        const bool can_device_api_hier_ar =
            useDeviceApiHierarchicalAllReducePoc() && can_device_api_rdma_ar &&
            hierarchicalAllReduceWorkspaceReady_ &&
            hierarchicalAllReduceRdmaReady_ &&
            hierarchicalAllReduceDeviceSequenceCounter_ &&
            hierarchicalAllReduceDeviceSequenceSlots_ && cuda_device_count > 0 &&
            meta_->activeSize == 2 * cuda_device_count &&
            (tensor.scalar_type() == c10::kFloat ||
             tensor.scalar_type() == c10::kBFloat16) &&
            hier_slot_stride > 0 && tensorSize <= hier_slot_stride;
        if (can_device_api_hier_ar) {
            const uint32_t sequence = hierarchicalAllReduceSequence_;
            const uint32_t slot_capacity =
                directP2pDeviceSequenceSlotCapacity();
            TORCH_CHECK(hierarchicalAllReduceDeviceSequenceSlotCursor_ + 1 <
                            slot_capacity,
                        "Hierarchical all_reduce device sequence slot pool "
                        "exhausted; set "
                        "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
            const uint32_t* local_device_sequence_slot =
                hierarchicalAllReduceDeviceSequenceSlots_ +
                hierarchicalAllReduceDeviceSequenceSlotCursor_++;
            launchP2pReserveSequence(
                hierarchicalAllReduceDeviceSequenceCounter_,
                const_cast<uint32_t*>(local_device_sequence_slot), 1,
                stream.stream());
            hierarchicalAllReduceSequence_ += 1;
            const int node_size = cuda_device_count;
            const int local_rank = rank_ % node_size;
            const int node_base_rank = rank_ - local_rank;
            const int pair_local_rank = rank_ < node_size ? 0 : 1;
            const int peer_pair_local_rank = 1 - pair_local_rank;
            const int peer_rank =
                rank_ < node_size ? rank_ + node_size : rank_ - node_size;
            TORCH_CHECK(peer_rank >= 0 && peer_rank < meta_->activeSize,
                        "Invalid hierarchical all_reduce peer rank: rank=",
                        rank_, " peer=", peer_rank,
                        " active=", meta_->activeSize);
            TORCH_CHECK(tensorSize <= hier_slot_stride,
                        "Hierarchical all_reduce local P2P stage exceeds "
                        "slot stride: bytes=", tensorSize,
                        " slot_stride=", hier_slot_stride);
            if (replayTraceEnabled()) {
                LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE op=all_reduce "
                          << "path=device_api_hier_pair rank=" << rank_
                          << " backend_index=" << meta_->backendIndex
                          << " active=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " dtype=" << tensor.scalar_type()
                          << " seq_hint=" << sequence
                          << " slots=" << store_signal_slots
                          << " local_node_size=" << node_size
                          << " local_rank=" << local_rank
                          << " node_base_rank=" << node_base_rank
                          << " peer_rank=" << peer_rank
                          << " hier_slot_stride=" << hier_slot_stride
                          << " hier_buffer_bytes="
                          << hierarchicalAllReduceBufferBytes_;
            }
            ensureCudaDevice();
            maybeDrainBeforeDeviceApiCollective(
                worker_.get(), meta_.get(), "all_reduce_hier", rank_,
                tensorSize, sequence, stream.stream());
            const size_t local_data_bytes =
                hier_slot_stride * static_cast<size_t>(store_signal_slots) *
                static_cast<size_t>(node_size);
            const size_t local_control_offset =
                (local_data_bytes + 15) & ~static_cast<size_t>(15);
            const size_t local_control_bytes = 2 * hier_signal_bytes;
            const size_t pair_scratch_offset =
                (local_control_offset + local_control_bytes + 15) &
                ~static_cast<size_t>(15);
            launchP2pNodeLocalAllReduceSlottedKernel(
                tensor, hierarchicalAllReduceBuffer_,
                hierarchicalAllReduceP2pTransport_->peerPtrsTablePtr(),
                hierarchicalAllReduceP2pTransport_->availableTablePtr(),
                tensorSize, hier_slot_stride, store_signal_slots,
                local_control_offset, rank_, node_base_rank, local_rank, node_size,
                local_device_sequence_slot, stream.stream());
            TORCH_CHECK(hierarchicalAllReduceDeviceSequenceSlotCursor_ <
                            slot_capacity,
                        "Hierarchical all_reduce device sequence slot pool "
                        "exhausted; set "
                        "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
            const uint32_t* pair_device_sequence_slot =
                hierarchicalAllReduceDeviceSequenceSlots_ +
                hierarchicalAllReduceDeviceSequenceSlotCursor_++;
            const size_t pair_chunk_bytes =
                std::min(tensorSize,
                         (hier_slot_stride / tensor.element_size()) *
                             tensor.element_size());
            TORCH_CHECK(pair_chunk_bytes > 0,
                        "Hierarchical all_reduce pair chunk size is zero: "
                        "bytes=", tensorSize,
                        " hier_slot_stride=", hier_slot_stride,
                        " element_size=", tensor.element_size());
            const size_t pair_chunk_count =
                (tensorSize + pair_chunk_bytes - 1) / pair_chunk_bytes;
            launchP2pReserveSequence(
                hierarchicalAllReduceDeviceSequenceCounter_,
                const_cast<uint32_t*>(pair_device_sequence_slot),
                static_cast<uint32_t>(pair_chunk_count), stream.stream());
            hierarchicalAllReduceSequence_ +=
                static_cast<uint32_t>(pair_chunk_count);
            const size_t pair_data_bytes =
                hier_slot_stride * static_cast<size_t>(store_signal_slots) *
                2;
            const size_t pair_control_offset =
                (pair_scratch_offset + pair_data_bytes + 15) &
                ~static_cast<size_t>(15);
            const size_t pair_control_bytes = 3 * hier_signal_bytes;
            TORCH_CHECK(
                pair_control_offset + pair_control_bytes <=
                    hierarchicalAllReduceBufferBytes_,
                "Hierarchical all_reduce pair scratch exceeds buffer: offset=",
                pair_scratch_offset, " stride=", hier_slot_stride,
                " slots=", store_signal_slots,
                " data_bytes=", pair_data_bytes,
                " control_offset=", pair_control_offset,
                " control_bytes=", pair_control_bytes,
                " buffer=", hierarchicalAllReduceBufferBytes_,
                " signal_bytes=", hier_signal_bytes);
            launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernel(
                tensor, hierarchicalAllReduceBuffer_,
                hierarchicalAllReduceP2pTransport_->peerPtrsTablePtr(),
                hierarchicalAllReduceP2pTransport_->availableTablePtr(),
                hierarchicalAllReduceRdmaTransport_->raddrsPtr(),
                hierarchicalAllReduceRdmaTransport_->rkeysPtr(),
                hierarchicalAllReduceRdmaTransport_->qpDevCtxsPtr(),
                hierarchicalAllReduceRdmaQpsPerRank_, tensorSize,
                pair_chunk_bytes, hier_slot_stride,
                pair_scratch_offset, pair_control_offset, store_signal_slots,
                rank_, meta_->activeSize, pair_local_rank,
                peer_pair_local_rank, peer_rank, pair_device_sequence_slot,
                nullptr, static_cast<uint32_t>(pair_chunk_count),
                stream.stream());
            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(
                launch_err == cudaSuccess,
                "Device API hierarchical all_reduce path launch failed: ",
                cudaGetErrorString(launch_err));
            maybeSyncAfterDeviceApiCollective(
                "all_reduce_hier", rank_, tensorSize, sequence, false, 1,
                stream.stream());
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=device_api_hier_pair rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " peer_rank=" << peer_rank
                          << " local_node_size=" << node_size;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        if (can_device_api_rdma_ar) {
            const uint32_t sequence = directP2pAllgatherSequence_;
            const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
            TORCH_CHECK(directP2pDeviceSequenceSlotCursor_ + 1 < slot_capacity,
                        "Direct P2P device sequence slot pool exhausted; set "
                        "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
            const uint32_t* rs_device_sequence_slot =
                directP2pDeviceSequenceSlots_ +
                directP2pDeviceSequenceSlotCursor_++;
            const uint32_t* ag_device_sequence_slot =
                directP2pDeviceSequenceSlots_ +
                directP2pDeviceSequenceSlotCursor_++;
            const bool fuse_device_sequence_reserve =
                useFusedDirectP2pDeviceSequenceReserve();
            const bool use_chunked_device_api_ar =
                direct_ar_segment_bytes > rdma_ar_slot_stride;
            const size_t rdma_ar_chunk_bytes = use_chunked_device_api_ar
                ? ((rdma_ar_slot_stride / tensor.element_size()) *
                   tensor.element_size())
                : direct_ar_segment_bytes;
            TORCH_CHECK(rdma_ar_chunk_bytes > 0,
                        "Mooncake PG Device API all_reduce chunk size is zero: "
                        "segment_bytes=", direct_ar_segment_bytes,
                        " slot_stride=", rdma_ar_slot_stride,
                        " element_size=", tensor.element_size());
            const size_t rdma_ar_chunk_count = use_chunked_device_api_ar
                ? ((direct_ar_segment_bytes + rdma_ar_chunk_bytes - 1) /
                   rdma_ar_chunk_bytes)
                : 1;
                if (!fuse_device_sequence_reserve) {
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(rs_device_sequence_slot),
                        static_cast<uint32_t>(rdma_ar_chunk_count),
                        stream.stream());
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(ag_device_sequence_slot),
                        static_cast<uint32_t>(rdma_ar_chunk_count),
                        stream.stream());
                }
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(2 * rdma_ar_chunk_count);
            if (replayTraceEnabled()) {
                LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE op=all_reduce "
                          << "path=device_api_rdma_rsag rank=" << rank_
                          << " backend_index=" << meta_->backendIndex
                          << " active=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " dtype=" << tensor.scalar_type()
                          << " element_size=" << tensor.element_size()
                          << " seq_hint=" << sequence
                          << " slots=" << store_signal_slots
                          << " rdma_slot_stride=" << rdma_ar_slot_stride
                          << " segment_bytes=" << direct_ar_segment_bytes
                          << " chunked=" << use_chunked_device_api_ar
                          << " chunk_bytes=" << rdma_ar_chunk_bytes
                          << " chunks=" << rdma_ar_chunk_count
                          << " fused_sequence="
                          << fuse_device_sequence_reserve
                          << " capturing=" << is_capturing
                          << " contiguous=" << tensor.is_contiguous();
            }
            ensureCudaDevice();
            maybeDrainBeforeDeviceApiCollective(
                worker_.get(), meta_.get(), "all_reduce", rank_, tensorSize,
                sequence, stream.stream());

            const size_t segment_numel =
                direct_ar_segment_bytes / tensor.element_size();
            auto local_segment = at::from_blob(
                static_cast<char*>(tensor.data_ptr()) +
                    static_cast<size_t>(rank_) * direct_ar_segment_bytes,
                {static_cast<int64_t>(segment_numel)}, tensor.options());
            if (use_chunked_device_api_ar) {
                launchDeviceApiReduceScatterSlottedChunkedGraphKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    meta_->activeRanksDevice, directRdmaTransport_->raddrsPtr(),
                    directRdmaTransport_->rkeysPtr(),
                    directRdmaTransport_->qpDevCtxsPtr(),
                    directRdmaQpsPerRank_, direct_ar_segment_bytes,
                    rdma_ar_chunk_bytes, rdma_ar_slot_stride,
                    rdma_ar_send_base_offset, store_signal_slots, rank_,
                    meta_->activeSize, rs_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    static_cast<uint32_t>(rdma_ar_chunk_count),
                    stream.stream());
                launchDeviceApiAllgatherStoreSignalSlottedChunkedGraphKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    meta_->activeRanksDevice, directRdmaTransport_->raddrsPtr(),
                    directRdmaTransport_->rkeysPtr(),
                    directRdmaTransport_->qpDevCtxsPtr(),
                    directRdmaQpsPerRank_, direct_ar_segment_bytes,
                    rdma_ar_chunk_bytes, rdma_ar_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize,
                    ag_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    static_cast<uint32_t>(rdma_ar_chunk_count),
                    stream.stream());
            } else {
                launchDeviceApiReduceScatterSlottedGraphKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    meta_->activeRanksDevice, directRdmaTransport_->raddrsPtr(),
                    directRdmaTransport_->rkeysPtr(),
                    directRdmaTransport_->qpDevCtxsPtr(),
                    directRdmaQpsPerRank_, direct_ar_segment_bytes,
                    rdma_ar_slot_stride, rdma_ar_send_base_offset,
                    store_signal_slots, rank_, meta_->activeSize,
                    rs_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    1, stream.stream());
                launchDeviceApiAllgatherStoreSignalSlottedGraphKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    meta_->activeRanksDevice, directRdmaTransport_->raddrsPtr(),
                    directRdmaTransport_->rkeysPtr(),
                    directRdmaTransport_->qpDevCtxsPtr(),
                    directRdmaQpsPerRank_, direct_ar_segment_bytes,
                    rdma_ar_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, ag_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    1, stream.stream());
            }

            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Device API RDMA all_reduce RS+AG path launch failed: ",
                        cudaGetErrorString(launch_err));
            maybeLogReplayTraceSequenceSlots(
                "all_reduce", rank_, meta_->backendIndex, tensorSize, sequence,
                rs_device_sequence_slot, ag_device_sequence_slot,
                store_signal_slots, stream.stream());
            maybeSyncAfterDeviceApiCollective(
                "all_reduce", rank_, tensorSize, sequence,
                use_chunked_device_api_ar, rdma_ar_chunk_count,
                stream.stream());
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=device_api_rdma_rsag rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " segment_bytes=" << direct_ar_segment_bytes
                          << " seq=" << sequence
                          << " buffer_bytes=" << direct_buffer_size
                          << " rdma_slot_stride=" << rdma_ar_slot_stride
                          << " chunked=" << use_chunked_device_api_ar
                          << " chunks=" << rdma_ar_chunk_count
                          << " fused_sequence="
                          << fuse_device_sequence_reserve;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        if (can_device_api_p2p_full_ar) {
            const uint32_t sequence = directP2pAllgatherSequence_++;
            launchP2pAllReduceSlottedKernel(
                tensor, recv_buffer_[0],
                directP2pTransport_->peerPtrsTablePtr(),
                directP2pTransport_->availableTablePtr(), tensorSize,
                direct_slot_stride, store_signal_slots, rank_,
                meta_->activeSize, sequence, stream.stream(), true);
            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(
                launch_err == cudaSuccess,
                "Device API all_reduce full-buffer path launch failed: ",
                cudaGetErrorString(launch_err));
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=device_api_full rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " slot_stride=" << direct_slot_stride;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        const int ar_ring_channels = directP2pAllReduceRingChannels();
        const size_t ar_ring_signal_bytes =
            directP2pSignalBytesFor(ar_ring_channels);
        const bool can_direct_ar_ring = !direct_p2p_capture_disabled &&
            useDirectP2pAllReducePoc() && useDirectP2pAllReduceRingPoc() &&
            directP2pAllgatherReady_ && tensor.is_contiguous() &&
            tensor.scalar_type() == c10::kFloat &&
            meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && meta_->activeSize > 1 &&
            tensorSize % static_cast<size_t>(meta_->activeSize) == 0 &&
            2 * tensorSize + ar_ring_signal_bytes <= direct_buffer_size &&
            (!isCudaStreamCapturing(stream.stream()) || use_device_sequence);
        if (can_direct_ar_ring) {
            const uint32_t sequence = directP2pAllgatherSequence_;
            const uint32_t* ar_device_sequence_slot = nullptr;
            if (use_device_sequence) {
                const uint32_t slot_capacity =
                    directP2pDeviceSequenceSlotCapacity();
                TORCH_CHECK(directP2pDeviceSequenceSlotCursor_ < slot_capacity,
                            "Direct P2P device sequence slot pool exhausted; set "
                            "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
                ar_device_sequence_slot = directP2pDeviceSequenceSlots_ +
                    directP2pDeviceSequenceSlotCursor_++;
                launchP2pReserveSequence(
                    directP2pDeviceSequenceCounter_,
                    const_cast<uint32_t*>(ar_device_sequence_slot), 1,
                    stream.stream());
                launchP2pAllReduceRingGraphKernel(
                    tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    rank_, meta_->activeSize, ar_device_sequence_slot,
                    deviceRingSignalMode(), ar_ring_channels, stream.stream());
            } else {
                directP2pAllgatherSequence_++;
                launchP2pAllReduceRingKernel(
                    tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    rank_, meta_->activeSize, sequence, deviceRingSignalMode(),
                    ar_ring_channels, stream.stream());
            }

            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Direct P2P ring all_reduce path launch failed: ",
                        cudaGetErrorString(launch_err));
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=ring rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " channels=" << ar_ring_channels
                          << " device_sequence=" << use_device_sequence;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        if (can_direct_ar_rsag) {
            const size_t direct_chunk_bytes =
                ((direct_slot_stride / tensor.element_size()) *
                 tensor.element_size());
            const bool use_chunked_ar =
                direct_ar_segment_bytes > direct_slot_stride;
            const size_t direct_ar_chunk_bytes =
                use_chunked_ar ? direct_chunk_bytes : direct_ar_segment_bytes;
            const size_t direct_rs_chunk_count = use_chunked_ar
                ? ((direct_ar_segment_bytes + direct_ar_chunk_bytes - 1) /
                   direct_ar_chunk_bytes)
                : 1;
            const size_t direct_ag_chunk_count = direct_rs_chunk_count;
            const bool want_ar_direct_output_ag =
                useDirectP2pAllReduceDirectOutputAllgatherPoc() &&
                use_device_sequence && !use_chunked_ar;
            const bool use_fused_rsag =
                useDirectP2pAllReduceFusedRsAgPoc() && use_device_sequence &&
                !use_chunked_ar && !want_ar_direct_output_ag &&
                (tensor.scalar_type() == c10::kFloat ||
                 tensor.scalar_type() == c10::kBFloat16);
            const bool use_ar_ag_skip_self =
                useDirectP2pAllReduceAllgatherSkipSelfPoc() &&
                use_device_sequence && !use_chunked_ar &&
                !want_ar_direct_output_ag && !use_fused_rsag;
            const size_t direct_ag_sequence_count =
                want_ar_direct_output_ag ? 1 : direct_ag_chunk_count;
            const bool fuse_device_sequence_reserve =
                use_device_sequence && useFusedDirectP2pDeviceSequenceReserve() &&
                !use_chunked_ar && !want_ar_direct_output_ag &&
                !use_fused_rsag && !use_ar_ag_skip_self;
            const size_t segment_numel =
                direct_ar_segment_bytes / tensor.element_size();
            auto local_segment = at::from_blob(
                static_cast<char*>(tensor.data_ptr()) +
                    static_cast<size_t>(rank_) * direct_ar_segment_bytes,
                {static_cast<int64_t>(segment_numel)}, tensor.options());
            DirectP2pOutputPeerCache* ar_output_cache = nullptr;

            const uint32_t sequence = directP2pAllgatherSequence_;
            const uint32_t* rs_device_sequence_slot = nullptr;
            const uint32_t* ag_device_sequence_slot = nullptr;
            if (use_device_sequence) {
                const uint32_t slot_capacity =
                    directP2pDeviceSequenceSlotCapacity();
                TORCH_CHECK(directP2pDeviceSequenceSlotCursor_ + 1 <
                                slot_capacity,
                            "Direct P2P device sequence slot pool exhausted; set "
                            "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
                rs_device_sequence_slot = directP2pDeviceSequenceSlots_ +
                    directP2pDeviceSequenceSlotCursor_++;
                ag_device_sequence_slot = directP2pDeviceSequenceSlots_ +
                    directP2pDeviceSequenceSlotCursor_++;
                if (!fuse_device_sequence_reserve) {
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(rs_device_sequence_slot),
                        static_cast<uint32_t>(direct_rs_chunk_count),
                        stream.stream());
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(ag_device_sequence_slot),
                        static_cast<uint32_t>(direct_ag_sequence_count),
                        stream.stream());
                }
                directP2pAllgatherSequence_ += static_cast<uint32_t>(
                    direct_rs_chunk_count + direct_ag_sequence_count);
            } else {
                directP2pAllgatherSequence_ += static_cast<uint32_t>(
                    direct_rs_chunk_count + direct_ag_sequence_count);
            }

            const bool profile_ar_stages =
                directP2pAllReduceStageProfileEnabled() && rank_ == 0 &&
                directP2pAllReduceStageProfileCount_ <
                    directP2pAllReduceStageProfileLimit();
            if (directP2pAllReduceStageProfileEnabled() && rank_ == 0 &&
                directP2pAllReduceStageProfileCount_ == 0) {
                LOG(WARNING)
                    << "MOONCAKE_PG_AR_STAGE_PROFILE enabled bytes="
                    << tensorSize << " segment_bytes="
                    << direct_ar_segment_bytes << " chunked="
                    << use_chunked_ar << " fused_sequence="
                    << fuse_device_sequence_reserve;
            }
            cudaEvent_t ar_profile_start = nullptr;
            cudaEvent_t ar_profile_rs_done = nullptr;
            cudaEvent_t ar_profile_ag_done = nullptr;
            if (profile_ar_stages) {
                cudaEventCreate(&ar_profile_start);
                cudaEventCreate(&ar_profile_rs_done);
                cudaEventCreate(&ar_profile_ag_done);
                cudaEventRecord(ar_profile_start, stream.stream());
            }

            if (use_fused_rsag) {
                launchP2pAllReduceFusedRsAgSlottedGraphKernel(
                    tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize,
                    rs_device_sequence_slot, ag_device_sequence_slot,
                    stream.stream());
            } else if (use_chunked_ar && use_device_sequence) {
                launchP2pReduceScatterSlottedChunkedGraphKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_ar_chunk_bytes,
                    direct_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, rs_device_sequence_slot,
                    stream.stream());
            } else if (use_chunked_ar) {
                launchP2pReduceScatterSlottedChunkedKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_ar_chunk_bytes,
                    direct_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, sequence, stream.stream());
            } else if (use_device_sequence) {
                launchP2pReduceScatterSlottedGraphKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize,
                    rs_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    static_cast<uint32_t>(direct_rs_chunk_count),
                    stream.stream());
            } else {
                launchP2pReduceScatterSlottedKernel(
                    local_segment, tensor, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize, sequence,
                    stream.stream());
            }

            if (profile_ar_stages) {
                cudaEventRecord(ar_profile_rs_done, stream.stream());
            }

            const uint32_t ag_sequence =
                sequence + static_cast<uint32_t>(direct_rs_chunk_count);
            if (want_ar_direct_output_ag) {
                const auto output_ptr =
                    reinterpret_cast<uintptr_t>(tensor.data_ptr());
                CUdeviceptr base_ptr = 0;
                size_t alloc_size = 0;
                CUresult cu_err = cuMemGetAddressRange(
                    &base_ptr, &alloc_size,
                    reinterpret_cast<CUdeviceptr>(tensor.data_ptr()));
                TORCH_CHECK(cu_err == CUDA_SUCCESS,
                            "AR direct-output AG cuMemGetAddressRange failed: ",
                            static_cast<int>(cu_err));
                const uint64_t local_offset =
                    output_ptr - static_cast<uintptr_t>(base_ptr);
                TORCH_CHECK(local_offset + tensorSize <= alloc_size,
                            "AR direct-output AG tensor exceeds allocation: offset=",
                            local_offset, " tensor_bytes=", tensorSize,
                            " alloc_size=", alloc_size);
                cudaIpcMemHandle_t handle;
                cudaError_t ipc_err = cudaIpcGetMemHandle(
                    &handle, reinterpret_cast<void*>(base_ptr));
                TORCH_CHECK(ipc_err == cudaSuccess,
                            "AR direct-output AG cudaIpcGetMemHandle failed: ",
                            cudaGetErrorString(ipc_err));

                std::vector<uint8_t> local_bytes(
                    sizeof(cudaIpcMemHandle_t) + sizeof(uint64_t) * 2);
                std::memcpy(local_bytes.data(), &handle,
                            sizeof(cudaIpcMemHandle_t));
                std::memcpy(local_bytes.data() + sizeof(cudaIpcMemHandle_t),
                            &local_offset, sizeof(uint64_t));
                uint64_t local_alloc_size = static_cast<uint64_t>(alloc_size);
                std::memcpy(local_bytes.data() + sizeof(cudaIpcMemHandle_t) +
                                sizeof(uint64_t),
                            &local_alloc_size, sizeof(uint64_t));
                meta_->store->set(directP2pAllgatherOutputKey(
                                      meta_->backendIndex, sequence, rank_),
                                  local_bytes);

                std::vector<std::string> keys;
                keys.reserve(meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    keys.push_back(directP2pAllgatherOutputKey(
                        meta_->backendIndex, sequence, j));
                }
                BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                    std::chrono::milliseconds(1)));
                waiter.wait([&] { return meta_->store->check(keys); });

                struct PeerOutputMeta {
                    cudaIpcMemHandle_t handle;
                    uint64_t offset;
                    uint64_t alloc_size;
                };
                std::vector<PeerOutputMeta> peer_meta(meta_->activeSize);
                std::string cache_key = std::to_string(tensorSize) + ":" +
                    std::to_string(meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    auto data = meta_->store->get(keys[j]);
                    TORCH_CHECK(data.size() ==
                                    sizeof(cudaIpcMemHandle_t) +
                                        sizeof(uint64_t) * 2,
                                "Invalid AR direct-output AG handle size.");
                    std::memcpy(&peer_meta[j].handle, data.data(),
                                sizeof(cudaIpcMemHandle_t));
                    std::memcpy(&peer_meta[j].offset,
                                data.data() + sizeof(cudaIpcMemHandle_t),
                                sizeof(uint64_t));
                    std::memcpy(&peer_meta[j].alloc_size,
                                data.data() + sizeof(cudaIpcMemHandle_t) +
                                    sizeof(uint64_t),
                                sizeof(uint64_t));
                    TORCH_CHECK(peer_meta[j].offset + tensorSize <=
                                    peer_meta[j].alloc_size,
                                "AR direct-output AG peer tensor exceeds allocation.");
                    cache_key += ":" +
                        std::to_string(
                            directP2pIpcHandleFingerprint(peer_meta[j].handle)) +
                        ":" + std::to_string(peer_meta[j].offset) + ":" +
                        std::to_string(peer_meta[j].alloc_size);
                }

                auto cache_it =
                    directP2pAllReduceOutputPeerCache_.find(cache_key);
                if (cache_it == directP2pAllReduceOutputPeerCache_.end()) {
                    DirectP2pOutputPeerCache cache;
                    cache.peer_ptrs_host.assign(meta_->activeSize, nullptr);
                    cache.ready = true;
                    for (int j = 0; j < meta_->activeSize; ++j) {
                        if (j == rank_) {
                            cache.peer_ptrs_host[j] = tensor.data_ptr();
                            continue;
                        }
                        void* peer_base = nullptr;
                        ipc_err = cudaIpcOpenMemHandle(
                            &peer_base, peer_meta[j].handle,
                            cudaIpcMemLazyEnablePeerAccess);
                        if (ipc_err != cudaSuccess) {
                            cache.ready = false;
                            LOG(WARNING)
                                << "AR direct-output AG failed to open rank "
                                << j << " output handle: "
                                << cudaGetErrorString(ipc_err);
                            continue;
                        }
                        cache.peer_ptrs_host[j] =
                            static_cast<char*>(peer_base) + peer_meta[j].offset;
                    }
                    if (cache.ready) {
                        cudaError_t malloc_err = cudaMalloc(
                            &cache.peer_ptrs_dev,
                            static_cast<size_t>(meta_->activeSize) *
                                sizeof(void*));
                        TORCH_CHECK(malloc_err == cudaSuccess,
                                    "AR direct-output AG peer table cudaMalloc failed: ",
                                    cudaGetErrorString(malloc_err));
                        cudaError_t table_err = cudaMemcpy(
                            cache.peer_ptrs_dev, cache.peer_ptrs_host.data(),
                            static_cast<size_t>(meta_->activeSize) *
                                sizeof(void*),
                            cudaMemcpyHostToDevice);
                        TORCH_CHECK(table_err == cudaSuccess,
                                    "AR direct-output AG peer table copy failed: ",
                                    cudaGetErrorString(table_err));
                    }
                    cache_it = directP2pAllReduceOutputPeerCache_
                                   .emplace(std::move(cache_key),
                                            std::move(cache))
                                   .first;
                }
                if (cache_it->second.ready) ar_output_cache = &cache_it->second;
            }

            if (use_fused_rsag) {
                // Fused RS+AG already wrote all output segments.
            } else if (ar_output_cache) {
                launchP2pAllgatherDirectOutputSlottedGraphKernel(
                    local_segment.data_ptr(), ar_output_cache->peer_ptrs_dev,
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, store_signal_slots, rank_,
                    meta_->activeSize, ag_device_sequence_slot,
                    stream.stream());
            } else if (use_chunked_ar && use_device_sequence) {
                launchP2pAllgatherStoreSignalSlottedChunkedGraphKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_ar_chunk_bytes,
                    direct_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, ag_device_sequence_slot,
                    stream.stream());
            } else if (use_chunked_ar) {
                launchP2pAllgatherStoreSignalSlottedChunkedKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_ar_chunk_bytes,
                    direct_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, ag_sequence, stream.stream());
            } else if (use_device_sequence && use_ar_ag_skip_self) {
                launchP2pAllgatherStoreSignalSlottedGraphSkipSelfKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize,
                    ag_device_sequence_slot, stream.stream());
            } else if (use_device_sequence) {
                launchP2pAllgatherStoreSignalSlottedGraphKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize,
                    ag_device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    static_cast<uint32_t>(direct_ag_chunk_count),
                    stream.stream());
            } else {
                launchP2pAllgatherStoreSignalSlottedKernel(
                    local_segment.data_ptr(), tensor.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(),
                    direct_ar_segment_bytes, direct_slot_stride,
                    store_signal_slots, rank_, meta_->activeSize, ag_sequence,
                    stream.stream());
            }

            if (profile_ar_stages) {
                cudaEventRecord(ar_profile_ag_done, stream.stream());
                cudaEventSynchronize(ar_profile_ag_done);
                float rs_ms = 0.0f;
                float ag_ms = 0.0f;
                float total_ms = 0.0f;
                cudaEventElapsedTime(&rs_ms, ar_profile_start,
                                     ar_profile_rs_done);
                cudaEventElapsedTime(&ag_ms, ar_profile_rs_done,
                                     ar_profile_ag_done);
                cudaEventElapsedTime(&total_ms, ar_profile_start,
                                     ar_profile_ag_done);
                LOG(WARNING)
                    << "MOONCAKE_PG_AR_STAGE_PROFILE rank=" << rank_
                    << " bytes=" << tensorSize
                    << " segment_bytes=" << direct_ar_segment_bytes
                    << " chunked=" << use_chunked_ar
                    << " device_sequence=" << use_device_sequence
                    << " fused_sequence=" << fuse_device_sequence_reserve
                    << " rs_us=" << (rs_ms * 1000.0f)
                    << " ag_us=" << (ag_ms * 1000.0f)
                    << " total_us=" << (total_ms * 1000.0f);
                cudaEventDestroy(ar_profile_start);
                cudaEventDestroy(ar_profile_rs_done);
                cudaEventDestroy(ar_profile_ag_done);
                directP2pAllReduceStageProfileCount_++;
            }

            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Direct P2P all_reduce RS+AG path launch failed: ",
                        cudaGetErrorString(launch_err));
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=fast_rsag rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " segment_bytes=" << direct_ar_segment_bytes
                          << " seq=" << sequence
                          << " buffer_bytes=" << direct_buffer_size
                          << " device_sequence=" << use_device_sequence
                          << " chunked=" << use_chunked_ar
                          << " fused_rsag=" << use_fused_rsag
                          << " ag_skip_self=" << use_ar_ag_skip_self
                          << " rs_chunks=" << direct_rs_chunk_count
                          << " ag_chunks=" << direct_ag_chunk_count;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        if (!useDirectP2pDeviceSequence() && !direct_p2p_capture_disabled &&
            useDirectP2pAllReducePoc() &&
            directP2pAllgatherReady_ &&
            tensor.is_contiguous() && meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && tensorSize <= direct_slot_stride &&
            tensorSize <= directP2pAllReduceMaxBytes()) {
            const uint32_t sequence = directP2pAllgatherSequence_++;
            launchP2pAllReduceSlottedKernel(
                tensor, recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                directP2pTransport_->availableTablePtr(), tensorSize,
                direct_slot_stride, store_signal_slots, rank_, meta_->activeSize,
                sequence, stream.stream());
            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Direct P2P all_reduce device path launch failed: ",
                        cudaGetErrorString(launch_err));
            if (shouldLogDirectP2pCount(g_directP2pArFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                          << "path=fast rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence;
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::ALLREDUCE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::ALLREDUCE, event_end);
        }
        if (useDirectP2pAllReducePoc() &&
            shouldLogDirectP2pCount(g_directP2pArFallbackCount)) {
            LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_reduce "
                      << "path=fallback rank=" << rank_
                      << " size=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " ready=" << directP2pAllgatherReady_
                      << " contiguous=" << tensor.is_contiguous()
                      << " capture_disabled=" << direct_p2p_capture_disabled
                      << " full_active="
                      << (meta_->activeSize == meta_->size)
                      << " sum=" << (opts.reduceOp == c10d::ReduceOp::SUM)
                      << " buffer_bytes=" << direct_buffer_size
                      << " slot_stride=" << direct_slot_stride
                      << " max_bytes=" << directP2pAllReduceMaxBytes();
        }
        return worker_->putTaskCuda(
            c10d::OpType::ALLREDUCE, tensorSize, 0, meta_, connection_ctx_,
            stream,
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
                                   enq_stream);
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
    if (isCpu_) {
        return worker_->putTaskCpu(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            connection_ctx_,
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
        const bool is_capturing = isCudaStreamCapturing(stream.stream());
        if (captureTraceEnabled()) {
            LOG(INFO) << "MOONCAKE_PG_CAPTURE_TRACE op=all_gather_base rank="
                      << rank_ << " backend_index=" << meta_->backendIndex
                      << " active=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " input_dtype=" << inputBuffer.scalar_type()
                      << " output_dtype=" << outputBuffer.scalar_type()
                      << " capturing=" << is_capturing
                      << " input_contiguous=" << inputBuffer.is_contiguous()
                      << " output_contiguous=" << outputBuffer.is_contiguous();
        }
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            is_capturing;
        const bool use_device_ring = useDeviceRingAllgatherPoc();
        const bool use_fifo_ring = useDeviceFifoRingAllgatherPoc();
        const bool use_store_signal = useDeviceStoreSignalAllgatherPoc();
        const bool use_store_signal_slotted =
            useDeviceStoreSignalSlottedAllgatherPoc();
        const int store_signal_slots = use_store_signal_slotted
            ? deviceStoreSignalSlots()
            : 1;
        const size_t direct_buffer_size = directP2pEffectiveBufferSize();
        const int ring_channels = use_device_ring ? deviceRingAllgatherChannels() : 1;
        const int fifo_slots = use_fifo_ring
            ? deviceRingAllgatherFifoSlots(meta_->activeSize)
            : 1;
        const size_t direct_signal_bytes = use_store_signal_slotted
            ? directP2pSlottedControlBytes(
                  store_signal_slots, useDeviceApiCollectivesPoc())
            : (use_fifo_ring
                   ? directP2pFifoSignalBytesFor(ring_channels, fifo_slots)
                   : directP2pSignalBytesFor(ring_channels));
        const size_t direct_slot_stride = use_store_signal_slotted
            ? directP2pSlotStride(direct_buffer_size, direct_signal_bytes,
                                  store_signal_slots, meta_->activeSize)
            : 0;
        const size_t direct_data_bytes = use_store_signal_slotted
            ? tensorSize
            : (use_fifo_ring
                   ? directP2pFifoBytesFor(tensorSize, ring_channels,
                                           fifo_slots)
                   : tensorSize * static_cast<size_t>(meta_->activeSize));
        const bool use_chunked_slotted = use_store_signal_slotted &&
            useDirectP2pChunked() && tensorSize > direct_slot_stride &&
            direct_slot_stride > 0;
        const size_t direct_chunk_bytes = use_chunked_slotted
            ? direct_slot_stride
            : tensorSize;
        const size_t direct_chunk_count = use_chunked_slotted
            ? ((tensorSize + direct_chunk_bytes - 1) / direct_chunk_bytes)
            : 1;
        auto input_ptr = reinterpret_cast<uintptr_t>(inputBuffer.data_ptr());
        auto output_ptr = reinterpret_cast<uintptr_t>(outputBuffer.data_ptr());
        const size_t outputSize = outputBuffer.numel() * outputBuffer.element_size();
        const bool overlaps_output =
            input_ptr >= output_ptr && input_ptr < output_ptr + outputSize;
        const bool use_direct_output = useDirectP2pOutputAllgatherPoc() &&
            !overlaps_output && use_store_signal_slotted;
        const bool use_device_sequence = useDirectP2pDeviceSequence() &&
            use_store_signal_slotted && directP2pDeviceSequenceCounter_ &&
            directP2pDeviceSequenceSlots_;
        const bool use_device_api_rdma_ag =
            useDeviceApiCollectivesPoc() && directRdmaReady_ &&
            use_store_signal_slotted && use_device_sequence &&
            !use_direct_output;
        const bool use_device_api_p2p_ag =
            useDeviceApiCollectivesPoc() && !directRdmaReady_ &&
            directP2pAllgatherReady_ && use_store_signal_slotted &&
            use_device_sequence && !use_direct_output &&
            allActivePeersOnSameHost() && !forceDisableDirectP2pPeers();
        const bool device_api_ag_requested =
            useDeviceApiCollectivesPoc() && useDirectP2pAllgatherPoc();
        if (device_api_ag_requested) {
            TORCH_CHECK(
                !direct_p2p_capture_disabled &&
                    (use_device_api_rdma_ag || use_device_api_p2p_ag) &&
                    inputBuffer.is_contiguous() && outputBuffer.is_contiguous() &&
                    meta_->activeSize == meta_->size &&
                    (!overlaps_output || use_store_signal ||
                     use_store_signal_slotted) &&
                    (tensorSize <= direct_slot_stride || use_chunked_slotted),
                "Mooncake PG Device API all_gather_base requested but path is "
                "not ready: capture_disabled=", direct_p2p_capture_disabled,
                " rdma_ready=", directRdmaReady_,
                " p2p_ready=", directP2pAllgatherReady_,
                " same_host=", allActivePeersOnSameHost(),
                " slotted=", use_store_signal_slotted,
                " device_sequence=", use_device_sequence,
                " chunked=", use_chunked_slotted,
                " direct_output=", use_direct_output,
                " input_contiguous=", inputBuffer.is_contiguous(),
                " output_contiguous=", outputBuffer.is_contiguous(),
                " full_active=", meta_->activeSize == meta_->size,
                " overlaps_output=", overlaps_output,
                " bytes=", tensorSize,
                " slot_stride=", direct_slot_stride);
        }
        const bool direct_device_collective_ready =
            device_api_ag_requested
                ? (use_device_api_rdma_ag || use_device_api_p2p_ag)
                                    : directP2pAllgatherReady_;
        if (!direct_p2p_capture_disabled && direct_device_collective_ready &&
            inputBuffer.is_contiguous() &&
            outputBuffer.is_contiguous() && meta_->activeSize == meta_->size &&
            (!overlaps_output || use_store_signal || use_store_signal_slotted) &&
            (use_store_signal_slotted
                 ? (tensorSize <= direct_slot_stride || use_chunked_slotted)
                 : direct_data_bytes + direct_signal_bytes <= direct_buffer_size)) {
            const bool use_current_stream =
                use_store_signal || use_store_signal_slotted ||
                useDirectP2pCurrentStream();
            at::cuda::CUDAStream enq_stream = use_current_stream
                ? stream
                : at::cuda::getStreamFromPool(false, inputBuffer.device().index());
            std::shared_ptr<torch::Event> event_start;
            if (!use_current_stream) {
                event_start = std::make_shared<torch::Event>(torch::kCUDA);
                event_start->record(stream);
                event_start->block(enq_stream);
            }
            const uint32_t sequence = directP2pAllgatherSequence_;
            const uint32_t* device_sequence_slot = nullptr;
            const bool fuse_device_sequence_reserve =
                use_device_sequence && useFusedDirectP2pDeviceSequenceReserve() &&
                !use_chunked_slotted && !use_direct_output;
            if (use_device_sequence) {
                const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
                TORCH_CHECK(directP2pDeviceSequenceSlotCursor_ < slot_capacity,
                            "Direct P2P device sequence slot pool exhausted; set "
                            "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
                device_sequence_slot = directP2pDeviceSequenceSlots_ +
                    directP2pDeviceSequenceSlotCursor_++;
                if (!fuse_device_sequence_reserve) {
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(device_sequence_slot),
                        static_cast<uint32_t>(direct_chunk_count),
                        enq_stream.stream());
                }
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            } else {
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            }
            if (directP2pAllgatherDebug()) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG_DEBUG rank=" << rank_
                          << " seq=" << sequence
                          << " stage=device_collective_enqueue";
            }
            if (replayTraceEnabled()) {
                LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE op=all_gather_base "
                          << "path="
                          << (use_device_api_rdma_ag ? "device_api_rdma"
                                                     : "p2p")
                          << " rank=" << rank_
                          << " backend_index=" << meta_->backendIndex
                          << " active=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " output_bytes="
                          << outputBuffer.numel() *
                                 outputBuffer.element_size()
                          << " input_dtype=" << inputBuffer.scalar_type()
                          << " output_dtype=" << outputBuffer.scalar_type()
                          << " element_size=" << inputBuffer.element_size()
                          << " seq_hint=" << sequence
                          << " slots=" << store_signal_slots
                          << " slot_stride=" << direct_slot_stride
                          << " chunked=" << use_chunked_slotted
                          << " chunk_bytes=" << direct_chunk_bytes
                          << " chunks=" << direct_chunk_count
                          << " fused_sequence="
                          << fuse_device_sequence_reserve
                          << " capturing=" << is_capturing
                          << " overlaps_output=" << overlaps_output
                          << " contiguous="
                          << (inputBuffer.is_contiguous() &&
                              outputBuffer.is_contiguous());
            }
            if (use_device_api_rdma_ag) {
                ensureCudaDevice();
                maybeDrainBeforeDeviceApiCollective(
                    worker_.get(), meta_.get(), "all_gather_base", rank_,
                    tensorSize, sequence, enq_stream.stream());
            }
            DirectP2pOutputPeerCache* output_cache = nullptr;
            if (use_direct_output) {
                auto cache_it = directP2pOutputPeerCache_.find(output_ptr);
                if (cache_it == directP2pOutputPeerCache_.end()) {
                    DirectP2pOutputPeerCache cache;
                    CUdeviceptr base_ptr = 0;
                    size_t alloc_size = 0;
                    CUresult cu_err = cuMemGetAddressRange(
                        &base_ptr, &alloc_size,
                        reinterpret_cast<CUdeviceptr>(outputBuffer.data_ptr()));
                    TORCH_CHECK(cu_err == CUDA_SUCCESS,
                                "Direct output P2P cuMemGetAddressRange failed: ",
                                static_cast<int>(cu_err));
                    const uint64_t local_offset =
                        output_ptr - static_cast<uintptr_t>(base_ptr);
                    cudaIpcMemHandle_t handle;
                    cudaError_t ipc_err = cudaIpcGetMemHandle(
                        &handle, reinterpret_cast<void*>(base_ptr));
                    TORCH_CHECK(ipc_err == cudaSuccess,
                                "Direct output P2P cudaIpcGetMemHandle failed: ",
                                cudaGetErrorString(ipc_err));

                    std::vector<uint8_t> local_bytes(
                        sizeof(cudaIpcMemHandle_t) + sizeof(uint64_t));
                    std::memcpy(local_bytes.data(), &handle,
                                sizeof(cudaIpcMemHandle_t));
                    std::memcpy(local_bytes.data() + sizeof(cudaIpcMemHandle_t),
                                &local_offset, sizeof(uint64_t));
                    meta_->store->set(directP2pAllgatherOutputKey(
                                          meta_->backendIndex, sequence, rank_),
                                      local_bytes);

                    std::vector<std::string> keys;
                    keys.reserve(meta_->activeSize);
                    for (int j = 0; j < meta_->activeSize; ++j) {
                        keys.push_back(directP2pAllgatherOutputKey(
                            meta_->backendIndex, sequence, j));
                    }
                    BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                        std::chrono::milliseconds(1)));
                    waiter.wait([&] { return meta_->store->check(keys); });

                    cache.peer_ptrs_host.assign(meta_->activeSize, nullptr);
                    cache.ready = true;
                    for (int j = 0; j < meta_->activeSize; ++j) {
                        auto data = meta_->store->get(keys[j]);
                        TORCH_CHECK(data.size() ==
                                        sizeof(cudaIpcMemHandle_t) +
                                            sizeof(uint64_t),
                                    "Invalid direct output P2P handle size.");
                        cudaIpcMemHandle_t peer_handle;
                        uint64_t peer_offset = 0;
                        std::memcpy(&peer_handle, data.data(),
                                    sizeof(cudaIpcMemHandle_t));
                        std::memcpy(&peer_offset,
                                    data.data() + sizeof(cudaIpcMemHandle_t),
                                    sizeof(uint64_t));
                        if (j == rank_) {
                            cache.peer_ptrs_host[j] = outputBuffer.data_ptr();
                            continue;
                        }
                        void* peer_base = nullptr;
                        ipc_err = cudaIpcOpenMemHandle(
                            &peer_base, peer_handle,
                            cudaIpcMemLazyEnablePeerAccess);
                        if (ipc_err != cudaSuccess) {
                            cache.ready = false;
                            LOG(WARNING)
                                << "Direct output P2P failed to open rank " << j
                                << " output handle: "
                                << cudaGetErrorString(ipc_err);
                            continue;
                        }
                        cache.peer_ptrs_host[j] =
                            static_cast<char*>(peer_base) + peer_offset;
                    }
                    if (cache.ready) {
                        cudaError_t malloc_err = cudaMalloc(
                            &cache.peer_ptrs_dev,
                            static_cast<size_t>(meta_->activeSize) *
                                sizeof(void*));
                        TORCH_CHECK(malloc_err == cudaSuccess,
                                    "Direct output P2P peer table cudaMalloc failed: ",
                                    cudaGetErrorString(malloc_err));
                        cudaError_t table_err = cudaMemcpy(
                            cache.peer_ptrs_dev, cache.peer_ptrs_host.data(),
                            static_cast<size_t>(meta_->activeSize) *
                                sizeof(void*),
                            cudaMemcpyHostToDevice);
                        TORCH_CHECK(table_err == cudaSuccess,
                                    "Direct output P2P peer table copy failed: ",
                                    cudaGetErrorString(table_err));
                    }
                    cache_it = directP2pOutputPeerCache_
                                   .emplace(output_ptr, std::move(cache))
                                   .first;
                }
                if (cache_it->second.ready) output_cache = &cache_it->second;
            }

            if (output_cache && use_device_sequence) {
                launchP2pAllgatherDirectOutputStoreSignalGraphKernel(
                    inputBuffer.data_ptr(), output_cache->peer_ptrs_dev,
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize, rank_,
                    meta_->activeSize, device_sequence_slot, enq_stream.stream());
            } else if (output_cache) {
                launchP2pAllgatherDirectOutputStoreSignalKernel(
                    inputBuffer.data_ptr(), output_cache->peer_ptrs_dev,
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize, rank_,
                    meta_->activeSize, sequence, enq_stream.stream());
            } else if (use_store_signal) {
                launchP2pAllgatherStoreSignalKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    rank_, meta_->activeSize, sequence, enq_stream.stream());
            } else if (use_chunked_slotted && use_device_sequence) {
                if (use_device_api_rdma_ag) {
                    launchDeviceApiAllgatherStoreSignalSlottedChunkedGraphKernel(
                        inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                        recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(),
                        meta_->activeRanksDevice,
                        directRdmaTransport_->raddrsPtr(),
                        directRdmaTransport_->rkeysPtr(),
                        directRdmaTransport_->qpDevCtxsPtr(),
                        directRdmaQpsPerRank_, tensorSize, direct_chunk_bytes,
                        direct_slot_stride, store_signal_slots, rank_,
                        meta_->activeSize, device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        enq_stream.stream());
                } else {
                    launchP2pAllgatherStoreSignalSlottedChunkedGraphKernel(
                        inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                        recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(), tensorSize,
                        direct_chunk_bytes, direct_slot_stride,
                        store_signal_slots, rank_, meta_->activeSize,
                        device_sequence_slot, enq_stream.stream());
                }
            } else if (use_chunked_slotted) {
                launchP2pAllgatherStoreSignalSlottedChunkedKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, sequence, enq_stream.stream());
            } else if (use_store_signal_slotted && use_device_sequence) {
                if (use_device_api_rdma_ag) {
                    launchDeviceApiAllgatherStoreSignalSlottedGraphKernel(
                        inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                        recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(),
                        meta_->activeRanksDevice,
                        directRdmaTransport_->raddrsPtr(),
                        directRdmaTransport_->rkeysPtr(),
                        directRdmaTransport_->qpDevCtxsPtr(),
                        directRdmaQpsPerRank_, tensorSize,
                        direct_slot_stride, store_signal_slots, rank_,
                        meta_->activeSize, device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        enq_stream.stream());
                } else {
                    launchP2pAllgatherStoreSignalSlottedGraphKernel(
                        inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                        recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(), tensorSize,
                        direct_slot_stride, store_signal_slots, rank_,
                        meta_->activeSize, device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        enq_stream.stream());
                }
            } else if (use_store_signal_slotted) {
                launchP2pAllgatherStoreSignalSlottedKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_slot_stride, store_signal_slots, rank_,
                    meta_->activeSize, sequence, enq_stream.stream());
            } else if (use_fifo_ring) {
                launchP2pAllgatherFifoRingKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    rank_, meta_->activeSize, sequence,
                    deviceRingSignalMode(), ring_channels, fifo_slots,
                    enq_stream.stream());
            } else if (use_device_ring) {
                launchP2pAllgatherRingKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    rank_, meta_->activeSize, sequence,
                    deviceRingSignalMode(), ring_channels,
                    enq_stream.stream());
            } else {
                for (int j = 0; j < meta_->activeSize; ++j) {
                    void* peer_base = directP2pPeerPtrsHost_[j];
                    TORCH_CHECK(
                        peer_base,
                        "Direct P2P allgather missing peer pointer for rank ",
                        j);
                    cudaError_t copy_launch_err = cudaMemcpyAsync(
                        static_cast<char*>(peer_base) +
                            static_cast<size_t>(rank_) * tensorSize,
                        inputBuffer.data_ptr(), tensorSize, cudaMemcpyDefault,
                        enq_stream.stream());
                    TORCH_CHECK(
                        copy_launch_err == cudaSuccess,
                        "Direct P2P allgather async peer copy launch failed: ",
                        cudaGetErrorString(copy_launch_err));
                }
                launchP2pAllgatherBaseFinishKernel(
                    outputBuffer.data_ptr(), recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize, rank_,
                    meta_->activeSize, sequence, enq_stream.stream());
            }
            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Direct P2P allgather device path launch failed: ",
                        cudaGetErrorString(launch_err));
            if (use_device_api_rdma_ag) {
                maybeSyncAfterDeviceApiCollective(
                    "all_gather_base", rank_, tensorSize, sequence,
                    use_chunked_slotted, direct_chunk_count,
                    enq_stream.stream());
            }
            if (shouldLogDirectP2pCount(g_directP2pAgFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_gather_base "
                          << "path=fast rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " buffer_bytes=" << direct_buffer_size
                          << " device_sequence=" << use_device_sequence
                          << " slotted=" << use_store_signal_slotted
                          << " chunked=" << use_chunked_slotted
                          << " chunks=" << direct_chunk_count
                          << " store_signal=" << use_store_signal
                          << " direct_output=" << static_cast<bool>(output_cache)
                          << " device_api_rdma=" << use_device_api_rdma_ag
                          << " no_event="
                          << (use_current_stream && useDirectP2pNoEventWork());
            }
            if (use_current_stream && useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::_ALLGATHER_BASE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(enq_stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::_ALLGATHER_BASE, event_end);
        }
        if (useDirectP2pAllgatherPoc() &&
            shouldLogDirectP2pCount(g_directP2pAgFallbackCount)) {
            LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=all_gather_base "
                      << "path=fallback rank=" << rank_
                      << " size=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " ready=" << directP2pAllgatherReady_
                      << " rdma_ready=" << directRdmaReady_
                      << " input_contiguous=" << inputBuffer.is_contiguous()
                      << " output_contiguous=" << outputBuffer.is_contiguous()
                      << " capture_disabled=" << direct_p2p_capture_disabled
                      << " full_active="
                      << (meta_->activeSize == meta_->size)
                      << " overlaps_output=" << overlaps_output
                      << " slotted=" << use_store_signal_slotted
                      << " chunked=" << use_chunked_slotted
                      << " data_bytes=" << direct_data_bytes
                      << " signal_bytes=" << direct_signal_bytes
                      << " buffer_bytes=" << direct_buffer_size
                      << " slot_stride=" << direct_slot_stride;
        }
        return worker_->putTaskCuda(
            c10d::OpType::_ALLGATHER_BASE, tensorSize, 0, meta_,
            connection_ctx_, stream,
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
    if (isCpu_) {
        auto numRanks = meta_->size;
        return worker_->putTaskCpu(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            connection_ctx_,
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
                                opts.reduceOp, meta_->activeRanks);
            });
    } else {
        auto stream =
            at::cuda::getCurrentCUDAStream(inputBuffer.device().index());
        const bool is_capturing = isCudaStreamCapturing(stream.stream());
        if (captureTraceEnabled()) {
            LOG(INFO) << "MOONCAKE_PG_CAPTURE_TRACE op=reduce_scatter_base rank="
                      << rank_ << " active=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " input_bytes="
                      << inputBuffer.numel() * inputBuffer.element_size()
                      << " output_dtype=" << outputBuffer.scalar_type()
                      << " capturing=" << is_capturing
                      << " input_contiguous=" << inputBuffer.is_contiguous()
                      << " output_contiguous=" << outputBuffer.is_contiguous()
                      << " reduce_op=" << static_cast<int>(opts.reduceOp);
        }
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            is_capturing;
        const int store_signal_slots = deviceStoreSignalSlots();
        const size_t direct_buffer_size = directP2pEffectiveBufferSize();
        const size_t direct_signal_bytes = directP2pSlottedControlBytes(
            store_signal_slots, useDeviceApiCollectivesPoc());
        const size_t direct_slot_stride = directP2pSlotStride(
            direct_buffer_size, direct_signal_bytes, store_signal_slots,
            meta_->activeSize);
        const bool use_device_sequence = useDirectP2pDeviceSequence() &&
            directP2pDeviceSequenceCounter_ && directP2pDeviceSequenceSlots_;
        const bool use_device_api_rdma_rs =
            useDeviceApiCollectivesPoc() && directRdmaReady_ &&
            use_device_sequence;
        const bool use_device_api_p2p_rs =
            useDeviceApiCollectivesPoc() && !directRdmaReady_ &&
            directP2pAllgatherReady_ && use_device_sequence &&
            allActivePeersOnSameHost() && !forceDisableDirectP2pPeers();
        const bool device_api_rs_requested =
            useDeviceApiCollectivesPoc() && useDirectP2pReduceScatterPoc();
        const size_t rdma_rs_slot_stride = use_device_api_rdma_rs
            ? directP2pSlotStride(direct_buffer_size, direct_signal_bytes,
                                  store_signal_slots, meta_->activeSize * 2)
            : direct_slot_stride;
        const size_t rdma_rs_send_base_offset =
            rdma_rs_slot_stride * static_cast<size_t>(store_signal_slots) *
            static_cast<size_t>(meta_->activeSize);
        const bool use_chunked_rs = useDirectP2pChunked() &&
            direct_slot_stride > 0 &&
            ((use_device_api_rdma_rs && tensorSize > rdma_rs_slot_stride) ||
             (!use_device_api_rdma_rs && tensorSize > direct_slot_stride));
        const size_t rs_chunk_slot_stride =
            use_device_api_rdma_rs ? rdma_rs_slot_stride : direct_slot_stride;
        const size_t direct_chunk_bytes = use_chunked_rs
            ? ((rs_chunk_slot_stride / outputBuffer.element_size()) *
               outputBuffer.element_size())
            : tensorSize;
        TORCH_CHECK(direct_chunk_bytes > 0,
                    "Mooncake PG reduce_scatter chunk size is zero: bytes=",
                    tensorSize, " slot_stride=", rs_chunk_slot_stride,
                    " element_size=", outputBuffer.element_size(),
                    " device_api_rdma=", use_device_api_rdma_rs);
        const size_t direct_chunk_count = use_chunked_rs
            ? ((tensorSize + direct_chunk_bytes - 1) / direct_chunk_bytes)
            : 1;
        if (device_api_rs_requested) {
            TORCH_CHECK(
                !direct_p2p_capture_disabled &&
                    (use_device_api_rdma_rs || use_device_api_p2p_rs) &&
                    inputBuffer.is_contiguous() && outputBuffer.is_contiguous() &&
                    meta_->activeSize == meta_->size &&
                    opts.reduceOp == c10d::ReduceOp::SUM &&
                    (tensorSize <= rdma_rs_slot_stride || use_chunked_rs) &&
                    inputBuffer.numel() * inputBuffer.element_size() >=
                        tensorSize * static_cast<size_t>(meta_->activeSize),
                "Mooncake PG Device API reduce_scatter_base requested but path "
                "is not ready: capture_disabled=", direct_p2p_capture_disabled,
                " rdma_ready=", directRdmaReady_,
                " p2p_ready=", directP2pAllgatherReady_,
                " same_host=", allActivePeersOnSameHost(),
                " device_sequence=", use_device_sequence,
                " input_contiguous=", inputBuffer.is_contiguous(),
                " output_contiguous=", outputBuffer.is_contiguous(),
                " full_active=", meta_->activeSize == meta_->size,
                " sum=", opts.reduceOp == c10d::ReduceOp::SUM,
                " bytes=", tensorSize,
                " rdma_slot_stride=", rdma_rs_slot_stride,
                " input_bytes=",
                inputBuffer.numel() * inputBuffer.element_size());
        }
        const bool direct_device_collective_ready =
            device_api_rs_requested ? (use_device_api_rdma_rs || use_device_api_p2p_rs)
                                    : directP2pAllgatherReady_;
        if (!direct_p2p_capture_disabled && useDirectP2pReduceScatterPoc() &&
            direct_device_collective_ready &&
            inputBuffer.is_contiguous() && outputBuffer.is_contiguous() &&
            meta_->activeSize == meta_->size && opts.reduceOp == c10d::ReduceOp::SUM &&
            (tensorSize <= rdma_rs_slot_stride || use_chunked_rs) &&
            inputBuffer.numel() * inputBuffer.element_size() >=
                tensorSize * static_cast<size_t>(meta_->activeSize)) {
            const uint32_t sequence = directP2pAllgatherSequence_;
            const uint32_t* device_sequence_slot = nullptr;
            const bool fuse_device_sequence_reserve =
                use_device_sequence && useFusedDirectP2pDeviceSequenceReserve() &&
                !use_chunked_rs;
            if (use_device_sequence) {
                const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
                TORCH_CHECK(directP2pDeviceSequenceSlotCursor_ < slot_capacity,
                            "Direct P2P device sequence slot pool exhausted; set "
                            "MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS larger.");
                device_sequence_slot = directP2pDeviceSequenceSlots_ +
                    directP2pDeviceSequenceSlotCursor_++;
                if (!fuse_device_sequence_reserve) {
                    launchP2pReserveSequence(
                        directP2pDeviceSequenceCounter_,
                        const_cast<uint32_t*>(device_sequence_slot),
                        static_cast<uint32_t>(direct_chunk_count),
                        stream.stream());
                }
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            } else {
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            }
            if (use_device_api_rdma_rs) {
                ensureCudaDevice();
                maybeDrainBeforeDeviceApiCollective(
                    worker_.get(), meta_.get(), "reduce_scatter_base", rank_,
                    tensorSize, sequence, stream.stream());
            }
            if (replayTraceEnabled()) {
                LOG(INFO) << "MOONCAKE_PG_REPLAY_TRACE op=reduce_scatter_base "
                          << "path="
                          << (use_device_api_rdma_rs ? "device_api_rdma"
                                                     : "p2p")
                          << " rank=" << rank_
                          << " backend_index=" << meta_->backendIndex
                          << " active=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " input_bytes="
                          << inputBuffer.numel() *
                                 inputBuffer.element_size()
                          << " output_dtype=" << outputBuffer.scalar_type()
                          << " input_dtype=" << inputBuffer.scalar_type()
                          << " element_size=" << outputBuffer.element_size()
                          << " seq_hint=" << sequence
                          << " slots=" << store_signal_slots
                          << " rdma_slot_stride=" << rdma_rs_slot_stride
                          << " slot_stride=" << rs_chunk_slot_stride
                          << " chunked=" << use_chunked_rs
                          << " chunk_bytes=" << direct_chunk_bytes
                          << " chunks=" << direct_chunk_count
                          << " fused_sequence="
                          << fuse_device_sequence_reserve
                          << " capturing=" << is_capturing
                          << " contiguous="
                          << (inputBuffer.is_contiguous() &&
                              outputBuffer.is_contiguous());
            }
            if (use_chunked_rs && use_device_sequence) {
                if (use_device_api_rdma_rs) {
                    launchDeviceApiReduceScatterSlottedChunkedGraphKernel(
                        outputBuffer, inputBuffer, recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(),
                        meta_->activeRanksDevice,
                        directRdmaTransport_->raddrsPtr(),
                        directRdmaTransport_->rkeysPtr(),
                        directRdmaTransport_->qpDevCtxsPtr(),
                        directRdmaQpsPerRank_, tensorSize, direct_chunk_bytes,
                        rdma_rs_slot_stride, rdma_rs_send_base_offset,
                        store_signal_slots, rank_, meta_->activeSize,
                        device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        stream.stream());
                } else {
                    launchP2pReduceScatterSlottedChunkedGraphKernel(
                        outputBuffer, inputBuffer, recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(), tensorSize,
                        direct_chunk_bytes, direct_slot_stride,
                        store_signal_slots, rank_, meta_->activeSize,
                        device_sequence_slot, stream.stream());
                }
            } else if (use_chunked_rs) {
                launchP2pReduceScatterSlottedChunkedKernel(
                    outputBuffer, inputBuffer, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, sequence, stream.stream());
            } else if (use_device_sequence) {
                if (use_device_api_rdma_rs) {
                    launchDeviceApiReduceScatterSlottedGraphKernel(
                        outputBuffer, inputBuffer, recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(),
                        meta_->activeRanksDevice,
                        directRdmaTransport_->raddrsPtr(),
                        directRdmaTransport_->rkeysPtr(),
                        directRdmaTransport_->qpDevCtxsPtr(),
                        directRdmaQpsPerRank_, tensorSize,
                        rdma_rs_slot_stride, rdma_rs_send_base_offset,
                        store_signal_slots, rank_, meta_->activeSize,
                        device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        stream.stream());
                } else {
                    launchP2pReduceScatterSlottedGraphKernel(
                        outputBuffer, inputBuffer, recv_buffer_[0],
                        directP2pTransport_->peerPtrsTablePtr(),
                        directP2pTransport_->availableTablePtr(), tensorSize,
                        direct_slot_stride, store_signal_slots, rank_,
                        meta_->activeSize, device_sequence_slot,
                        fuse_device_sequence_reserve
                            ? directP2pDeviceSequenceCounter_
                            : nullptr,
                        static_cast<uint32_t>(direct_chunk_count),
                        stream.stream());
                }
            } else {
                launchP2pReduceScatterSlottedKernel(
                    outputBuffer, inputBuffer, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_slot_stride, store_signal_slots, rank_, meta_->activeSize,
                    sequence, stream.stream());
            }
            cudaError_t launch_err = cudaGetLastError();
            TORCH_CHECK(launch_err == cudaSuccess,
                        "Direct P2P reduce_scatter device path launch failed: ",
                        cudaGetErrorString(launch_err));
            if (use_device_api_rdma_rs) {
                maybeSyncAfterDeviceApiCollective(
                    "reduce_scatter_base", rank_, tensorSize, sequence,
                    use_chunked_rs, direct_chunk_count, stream.stream());
            }
            if (shouldLogDirectP2pCount(g_directP2pRsFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=reduce_scatter_base "
                          << "path=fast rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " buffer_bytes=" << direct_buffer_size
                          << " device_sequence=" << use_device_sequence
                          << " device_api_rdma=" << use_device_api_rdma_rs
                          << " rdma_slot_stride=" << rdma_rs_slot_stride
                          << " chunked=" << use_chunked_rs
                          << " chunks=" << direct_chunk_count
                          << " no_event=" << useDirectP2pNoEventWork();
            }
            if (useDirectP2pNoEventWork()) {
                return c10::make_intrusive<MooncakeAlreadyEnqueuedWorkCuda>(
                    c10d::OpType::_REDUCE_SCATTER_BASE);
            }
            auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
            event_end->record(stream);
            return c10::make_intrusive<MooncakeEventWorkCuda>(
                c10d::OpType::_REDUCE_SCATTER_BASE, event_end);
        }
        if (useDirectP2pReduceScatterPoc() &&
            shouldLogDirectP2pCount(g_directP2pRsFallbackCount)) {
            LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=reduce_scatter_base "
                      << "path=fallback rank=" << rank_
                      << " size=" << meta_->activeSize
                      << " bytes=" << tensorSize
                      << " ready=" << directP2pAllgatherReady_
                      << " rdma_ready=" << directRdmaReady_
                      << " input_contiguous=" << inputBuffer.is_contiguous()
                      << " output_contiguous=" << outputBuffer.is_contiguous()
                      << " capture_disabled=" << direct_p2p_capture_disabled
                      << " full_active="
                      << (meta_->activeSize == meta_->size)
                      << " sum=" << (opts.reduceOp == c10d::ReduceOp::SUM)
                      << " buffer_bytes=" << direct_buffer_size
                      << " slot_stride=" << direct_slot_stride
                      << " rdma_slot_stride=" << rdma_rs_slot_stride
                      << " chunked=" << use_chunked_rs
                      << " input_bytes="
                      << inputBuffer.numel() * inputBuffer.element_size();
        }
        return worker_->putTaskCuda(
            c10d::OpType::_REDUCE_SCATTER_BASE, tensorSize, 0, meta_,
            connection_ctx_, stream,
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
                                   meta_->activeRanksDevice, enq_stream);
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
    if (isCpu_) {
        return worker_->putTaskCpu(
            // a non-zero tensorSize is required to ensure the worker task for
            // the barrier is created
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_,
            connection_ctx_, [=](void*, size_t, size_t) {},
            [=](void*, size_t, size_t) {});
    } else {
        auto device_index = at::cuda::current_device();
        auto stream = at::cuda::getCurrentCUDAStream(device_index);
        return worker_->putTaskCuda(
            c10d::OpType::BARRIER, kBarrierDummyTensorSize, 0, meta_,
            connection_ctx_, stream,
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
                                       enq_stream);
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

    // Phase 3: Drain warm-up transfers for connection poller
    connection_ctx_->shutdown();
    if (connectionPollerRegistered_) {
        ConnectionPoller::GetInstance().removeContext(connection_ctx_);
        has_hung_operation |= !connection_ctx_->drainPoller();
        connectionPollerRegistered_ = false;
    }

    // Phase 4: CUDA synchronization
    if (!isCpu_ && !has_hung_operation) {
        cudaDeviceSynchronize();
    }

    // Phase 5: Release resources if no hung operations
    if (has_hung_operation) {
        p2p_proxy_->abandonResources();
        connection_ctx_->abandonResources();
    }

    if (!has_hung_operation) {
        resetDeviceCollectiveRuntime();
        for (size_t i = 0; i < 2; i++) {
            engine_->unregisterLocalMemory(cpu_sync_send_region_[i]);
            engine_->unregisterLocalMemory(cpu_sync_recv_region_[i]);
            engine_->unregisterLocalMemory(send_buffer_[i]);
            engine_->unregisterLocalMemory(recv_buffer_[i]);
            if (syncRegionsOnDevice_) {
                cudaFree(cpu_sync_send_region_[i]);
                cudaFree(cpu_sync_recv_region_[i]);
            } else {
                delete[] cpu_sync_send_region_[i];
                delete[] cpu_sync_recv_region_[i];
            }
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
        if (directP2pDeviceSequenceCounter_) {
            cudaFree(directP2pDeviceSequenceCounter_);
            directP2pDeviceSequenceCounter_ = nullptr;
        }
        if (directP2pDeviceSequenceSlots_) {
            cudaFree(directP2pDeviceSequenceSlots_);
            directP2pDeviceSequenceSlots_ = nullptr;
        }
        if (hierarchicalAllReduceBuffer_) {
            cudaFree(hierarchicalAllReduceBuffer_);
            hierarchicalAllReduceBuffer_ = nullptr;
            hierarchicalAllReduceBufferBytes_ = 0;
        }
        if (hierarchicalAllReduceDeviceSequenceCounter_) {
            cudaFree(hierarchicalAllReduceDeviceSequenceCounter_);
            hierarchicalAllReduceDeviceSequenceCounter_ = nullptr;
        }
        if (hierarchicalAllReduceDeviceSequenceSlots_) {
            cudaFree(hierarchicalAllReduceDeviceSequenceSlots_);
            hierarchicalAllReduceDeviceSequenceSlots_ = nullptr;
        }
        hierarchicalAllReduceDeviceSequenceSlotCursor_ = 0;
        hierarchicalAllReduceSequence_ = 1;
        hierarchicalAllReduceP2pTransport_.reset();
        hierarchicalAllReduceRdmaTransport_.reset();
        hierarchicalAllReduceWorkspaceReady_ = false;
        hierarchicalAllReduceRdmaReady_ = false;
        meta_->activeRanks = nullptr;
        meta_->activeRanksDevice = nullptr;
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

bool MooncakeBackend::allActivePeersOnSameHost() const {
    if (!meta_ || !meta_->store) return false;

    const std::string local_server_host = serverHostOnly(localServerName_);
    std::vector<std::string> server_name_keys;
    server_name_keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        server_name_keys.push_back(
            ConnectionContext::getServerNameStoreKey(meta_->backendIndex, j));
    }
    BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
        std::chrono::milliseconds(10)));
    waiter.wait([&] { return meta_->store->check(server_name_keys); });

    for (int j = 0; j < meta_->activeSize; ++j) {
        if (!meta_->activeRanks[j]) continue;
        const auto peer_server_name = meta_->store->get(server_name_keys[j]);
        const std::string peer_server_name_str(peer_server_name.begin(),
                                               peer_server_name.end());
        if (serverHostOnly(peer_server_name_str) != local_server_host) {
            return false;
        }
    }
    return true;
}

void MooncakeBackend::maybeInitHierarchicalAllReduceWorkspace() {
#ifdef MOONCAKE_EP_USE_MUSA
    return;
#else
    if (isCpu_ || !useDeviceApiCollectivesPoc() ||
        !useDeviceApiHierarchicalAllReducePoc() || !directRdmaReady_) {
        return;
    }
    TORCH_CHECK(meta_->store,
                "Hierarchical all_reduce workspace requires a valid Store.");
    TORCH_CHECK(static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
                "Hierarchical all_reduce active size exceeds rank limit.");

    const bool debug = directP2pAllgatherDebug();
    auto debug_log = [&](const std::string& stage) {
        if (debug) {
            LOG(INFO) << "MOONCAKE_PG_HIER_AR_WORKSPACE debug rank=" << rank_
                      << " backend_index=" << meta_->backendIndex
                      << " stage=" << stage << " size=" << meta_->activeSize;
        }
    };

    hierarchicalAllReduceBufferBytes_ = directP2pEffectiveBufferSize();
    TORCH_CHECK(hierarchicalAllReduceBufferBytes_ > 0,
                "Hierarchical all_reduce workspace size must be positive.");
    cudaError_t alloc_err = cudaMalloc(&hierarchicalAllReduceBuffer_,
                                       hierarchicalAllReduceBufferBytes_);
    TORCH_CHECK(alloc_err == cudaSuccess,
                "Failed to allocate hierarchical all_reduce workspace: ",
                cudaGetErrorString(alloc_err));
    cudaError_t memset_err = cudaMemset(hierarchicalAllReduceBuffer_, 0,
                                        hierarchicalAllReduceBufferBytes_);
    TORCH_CHECK(memset_err == cudaSuccess,
                "Failed to clear hierarchical all_reduce workspace: ",
                cudaGetErrorString(memset_err));
    debug_log("buffer_ready");

    hierarchicalAllReduceP2pTransport_ =
        device::createP2pDeviceTransport(meta_->activeSize);
    TORCH_CHECK(hierarchicalAllReduceP2pTransport_,
                "Failed to create hierarchical all_reduce P2P transport.");
    auto local_handle = hierarchicalAllReduceP2pTransport_->exportIpcHandle(
        hierarchicalAllReduceBuffer_);
    std::vector<uint8_t> local_bytes(local_handle.size() * sizeof(int32_t));
    if (!local_handle.empty()) {
        std::memcpy(local_bytes.data(), local_handle.data(),
                    local_bytes.size());
    }
    meta_->store->set(hierarchicalAllReduceP2pKey(meta_->backendIndex, rank_),
                      local_bytes);
    debug_log("p2p_metadata_published");

    std::vector<std::string> p2p_keys;
    p2p_keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        p2p_keys.push_back(hierarchicalAllReduceP2pKey(meta_->backendIndex, j));
    }
    BackoffWaiter p2p_waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
    p2p_waiter.wait([&] { return meta_->store->check(p2p_keys); });
    debug_log("p2p_metadata_wait_done");

    std::vector<std::vector<int32_t>> remote_handles(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        auto data = meta_->store->get(p2p_keys[j]);
        TORCH_CHECK(data.size() % sizeof(int32_t) == 0,
                    "Invalid hierarchical all_reduce IPC handle size.");
        remote_handles[j].resize(data.size() / sizeof(int32_t));
        if (!data.empty()) {
            std::memcpy(remote_handles[j].data(), data.data(), data.size());
        }
    }
    std::vector<int> p2p_active_mask(meta_->activeSize, 1);
    hierarchicalAllReduceP2pTransport_->importPeerHandles(
        hierarchicalAllReduceBuffer_, rank_, meta_->activeSize, remote_handles,
        p2p_active_mask);
    debug_log("p2p_import_done");

    std::vector<std::string> rdma_filters = deviceFilters_;
    if (rdma_filters.empty()) rdma_filters = parseDeviceFilterEnv();
    hierarchicalAllReduceRdmaQpsPerRank_ = 1;
    const int num_qps =
        meta_->activeSize * hierarchicalAllReduceRdmaQpsPerRank_;
    hierarchicalAllReduceRdmaTransport_ =
        device::createIbgdaDeviceTransport(rdma_filters);
    int rc = hierarchicalAllReduceRdmaTransport_
                 ? hierarchicalAllReduceRdmaTransport_->initialize(
                       "", meta_->activeSize, num_qps)
                 : -1;
    if (rc == 0) {
        rc = hierarchicalAllReduceRdmaTransport_->registerMemory(
            hierarchicalAllReduceBuffer_, hierarchicalAllReduceBufferBytes_);
    }
    if (rc == 0) {
        rc = hierarchicalAllReduceRdmaTransport_->allocateControlBuffer();
    }
    if (rc == 0) {
        auto stream = at::cuda::getCurrentCUDAStream();
        rc = hierarchicalAllReduceRdmaTransport_->createQueuePairs(
            stream.stream());
    }
    if (rc == 0) {
        auto local_meta = hierarchicalAllReduceRdmaTransport_->localMetadata();
        meta_->store->set(hierarchicalAllReduceRdmaKey(meta_->backendIndex,
                                                       rank_),
                          serializeRdmaMetadata(local_meta));

        std::vector<std::string> rdma_keys;
        rdma_keys.reserve(meta_->activeSize);
        for (int j = 0; j < meta_->activeSize; ++j) {
            rdma_keys.push_back(
                hierarchicalAllReduceRdmaKey(meta_->backendIndex, j));
        }
        BackoffWaiter rdma_waiter(BackoffWaiterConfig::constantSleep(
            std::chrono::milliseconds(10)));
        rdma_waiter.wait([&] { return meta_->store->check(rdma_keys); });

        std::vector<device::RdmaLocalMetadata> remote_meta(meta_->activeSize);
        for (int j = 0; j < meta_->activeSize; ++j) {
            remote_meta[j] =
                deserializeRdmaMetadata(meta_->store->get(rdma_keys[j]));
            TORCH_CHECK(remote_meta[j].qpns.size() >=
                            static_cast<size_t>(num_qps),
                        "Hierarchical all_reduce RDMA metadata missing QPNs.");
            TORCH_CHECK(remote_meta[j].lids.size() >=
                            static_cast<size_t>(num_qps),
                        "Hierarchical all_reduce RDMA metadata missing LIDs.");
        }

        std::vector<int64_t> remote_addrs(meta_->activeSize, 0);
        std::vector<int32_t> remote_keys(meta_->activeSize, 0);
        std::vector<int64_t> subnet_prefixes(meta_->activeSize, 0);
        std::vector<int64_t> interface_ids(meta_->activeSize, 0);
        for (int j = 0; j < meta_->activeSize; ++j) {
            remote_addrs[j] = remote_meta[j].raddr;
            remote_keys[j] = remote_meta[j].rkey;
            subnet_prefixes[j] = remote_meta[j].subnet_prefix;
            interface_ids[j] = remote_meta[j].interface_id;
        }

        std::vector<int32_t> remote_qpns(num_qps, 0);
        std::vector<int32_t> remote_lids(num_qps, 0);
        for (int i = 0; i < num_qps; ++i) {
            const int peer_rank = i * meta_->activeSize / num_qps;
            const int channel = i % hierarchicalAllReduceRdmaQpsPerRank_;
            const int peer_local_qp =
                rank_ * hierarchicalAllReduceRdmaQpsPerRank_ + channel;
            remote_qpns[i] = remote_meta[peer_rank].qpns[peer_local_qp];
            remote_lids[i] = remote_meta[peer_rank].lids[peer_local_qp];
        }
        std::vector<int> rdma_active_mask(meta_->activeSize, 1);
        rc = hierarchicalAllReduceRdmaTransport_->connectPeers(
            rank_, hierarchicalAllReduceRdmaTransport_->isRoce(), remote_addrs,
            remote_keys, remote_qpns, remote_lids, subnet_prefixes,
            interface_ids, rdma_active_mask);
    }

    hierarchicalAllReduceRdmaReady_ = (rc == 0);
    hierarchicalAllReduceWorkspaceReady_ =
        hierarchicalAllReduceP2pTransport_ &&
        hierarchicalAllReduceRdmaReady_;
    if (!hierarchicalAllReduceWorkspaceReady_) {
        LOG(WARNING) << "MOONCAKE_PG_HIER_AR_WORKSPACE init failed rank="
                     << rank_ << " backend_index=" << meta_->backendIndex
                     << " rc=" << rc;
        hierarchicalAllReduceRdmaTransport_.reset();
        hierarchicalAllReduceRdmaReady_ = false;
    } else {
        const uint32_t initial_sequence = 1;
        cudaError_t seq_counter_err = cudaMalloc(
            &hierarchicalAllReduceDeviceSequenceCounter_, sizeof(uint32_t));
        TORCH_CHECK(
            seq_counter_err == cudaSuccess,
            "Failed to allocate hierarchical all_reduce sequence counter: ",
            cudaGetErrorString(seq_counter_err));
        cudaError_t seq_counter_copy_err = cudaMemcpy(
            hierarchicalAllReduceDeviceSequenceCounter_, &initial_sequence,
            sizeof(uint32_t), cudaMemcpyHostToDevice);
        TORCH_CHECK(
            seq_counter_copy_err == cudaSuccess,
            "Failed to initialize hierarchical all_reduce sequence counter: ",
            cudaGetErrorString(seq_counter_copy_err));
        const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
        cudaError_t seq_slots_err = cudaMalloc(
            &hierarchicalAllReduceDeviceSequenceSlots_,
            static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
        TORCH_CHECK(
            seq_slots_err == cudaSuccess,
            "Failed to allocate hierarchical all_reduce sequence slots: ",
            cudaGetErrorString(seq_slots_err));
        cudaError_t seq_slots_memset_err = cudaMemset(
            hierarchicalAllReduceDeviceSequenceSlots_, 0,
            static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
        TORCH_CHECK(
            seq_slots_memset_err == cudaSuccess,
            "Failed to clear hierarchical all_reduce sequence slots: ",
            cudaGetErrorString(seq_slots_memset_err));
        hierarchicalAllReduceSequence_ = initial_sequence;
        hierarchicalAllReduceDeviceSequenceSlotCursor_ = 0;
        LOG(INFO) << "MOONCAKE_PG_HIER_AR_WORKSPACE ready=1 rank=" << rank_
                  << " backend_index=" << meta_->backendIndex
                  << " size=" << meta_->activeSize
                  << " bytes=" << hierarchicalAllReduceBufferBytes_;
    }
#endif
}

void MooncakeBackend::maybeInitDirectP2pAllgather() {
#ifdef MOONCAKE_EP_USE_MUSA
    return;
#else
    if (isCpu_ || !useDirectP2pAllgatherPoc()) return;
    const bool debug = directP2pAllgatherDebug();
    auto debug_log = [&](const std::string& stage) {
        if (debug) {
            LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG debug rank=" << rank_
                      << " backend_index=" << meta_->backendIndex
                      << " stage=" << stage << " size=" << meta_->activeSize;
        }
    };
    debug_log("enter");
    TORCH_CHECK(meta_->store,
                "Direct P2P allgather PoC requires a valid Store.");
    TORCH_CHECK(static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
                "Direct P2P allgather active size exceeds rank limit.");

    cudaError_t memset_err = cudaMemset(recv_buffer_[0], 0, kBufferSize);
    TORCH_CHECK(memset_err == cudaSuccess,
                "Failed to clear direct P2P allgather buffer: ",
                cudaGetErrorString(memset_err));
    debug_log("buffer_cleared");

    directP2pTransport_ = device::createP2pDeviceTransport(meta_->activeSize);
    TORCH_CHECK(directP2pTransport_,
                "Failed to create direct P2P transport for allgather PoC.");
    auto local_handle = directP2pTransport_->exportIpcHandle(recv_buffer_[0]);
    std::vector<uint8_t> local_bytes(local_handle.size() * sizeof(int32_t));
    if (!local_handle.empty()) {
        std::memcpy(local_bytes.data(), local_handle.data(),
                    local_bytes.size());
    }
    meta_->store->set(directP2pAllgatherKey(meta_->backendIndex, rank_),
                      local_bytes);
    debug_log("p2p_metadata_published");

    std::vector<std::string> keys;
    keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        keys.push_back(directP2pAllgatherKey(meta_->backendIndex, j));
    }
    BackoffWaiter waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
    debug_log("p2p_metadata_wait_begin");
    waiter.wait([&] { return meta_->store->check(keys); });
    debug_log("p2p_metadata_wait_done");

    std::vector<std::vector<int32_t>> remote_handles(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        auto data = meta_->store->get(keys[j]);
        TORCH_CHECK(data.size() % sizeof(int32_t) == 0,
                    "Invalid direct P2P allgather IPC handle size.");
        remote_handles[j].resize(data.size() / sizeof(int32_t));
        if (!data.empty()) {
            std::memcpy(remote_handles[j].data(), data.data(), data.size());
        }
    }

    std::vector<int> active_mask(meta_->activeSize, 1);
    if (forceDisableDirectP2pPeers()) {
        const std::string local_server_host = serverHostOnly(localServerName_);
        std::vector<std::string> server_name_keys;
        server_name_keys.reserve(meta_->activeSize);
        for (int j = 0; j < meta_->activeSize; ++j) {
            server_name_keys.push_back(
                ConnectionContext::getServerNameStoreKey(meta_->backendIndex, j));
        }
        BackoffWaiter server_name_waiter(
            BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
        server_name_waiter.wait(
            [&] { return meta_->store->check(server_name_keys); });
        for (int j = 0; j < meta_->activeSize; ++j) {
            if (j == rank_) continue;
            const auto peer_server_name =
                meta_->store->get(server_name_keys[j]);
            const std::string peer_server_name_str(peer_server_name.begin(),
                                                   peer_server_name.end());
            if (serverHostOnly(peer_server_name_str) == local_server_host) {
                active_mask[j] = 0;
            }
        }
        if (debug) {
            std::stringstream mask_ss;
            for (int j = 0; j < meta_->activeSize; ++j) {
                if (j) mask_ss << ",";
                mask_ss << active_mask[j];
            }
            LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG debug rank=" << rank_
                      << " stage=force_disable_p2p active_mask="
                      << mask_ss.str();
        }
    }
    directP2pTransport_->importPeerHandles(
        recv_buffer_[0], rank_, meta_->activeSize, remote_handles, active_mask);
    debug_log("p2p_import_done");
    directP2pAllgatherReady_ = directP2pTransport_->allPeersAccessible();
    directP2pPeerPtrsHost_.assign(meta_->activeSize, nullptr);
    cudaError_t peer_copy_err = cudaMemcpy(
        directP2pPeerPtrsHost_.data(), directP2pTransport_->peerPtrsTablePtr(),
        directP2pPeerPtrsHost_.size() * sizeof(void*), cudaMemcpyDeviceToHost);
    TORCH_CHECK(peer_copy_err == cudaSuccess,
                "Failed to copy direct P2P peer pointer table: ",
                cudaGetErrorString(peer_copy_err));
    debug_log("p2p_peer_table_copied");

    const bool all_active_peers_on_same_host = allActivePeersOnSameHost();
    const bool should_skip_single_host_rdma =
        useDeviceApiCollectivesPoc() && directP2pAllgatherReady_ &&
        all_active_peers_on_same_host && !forceDisableDirectP2pPeers();
    if (debug && should_skip_single_host_rdma) {
        LOG(INFO) << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                  << " stage=skip_single_host_rdma backend_index="
                  << meta_->backendIndex
                  << " reason=all_active_peers_on_same_host";
    }

    if (useDeviceApiCollectivesPoc() && !should_skip_single_host_rdma) {
        ensureCudaDevice();
        std::vector<std::string> rdma_filters = deviceFilters_;
        if (rdma_filters.empty()) rdma_filters = parseDeviceFilterEnv();
        directRdmaQpsPerRank_ = 1;
        const int num_qps = meta_->activeSize * directRdmaQpsPerRank_;
        if (debug) {
            std::stringstream filters_ss;
            for (size_t i = 0; i < rdma_filters.size(); ++i) {
                if (i) filters_ss << ",";
                filters_ss << rdma_filters[i];
            }
            int current_device = -1;
            cudaGetDevice(&current_device);
            LOG(INFO) << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                      << " stage=create_transport num_qps=" << num_qps
                      << " filters=" << filters_ss.str()
                      << " saved_cuda_device=" << cudaDeviceIndex_
                      << " current_device=" << current_device
                      << " backend_index=" << meta_->backendIndex
                      << " size=" << meta_->activeSize;
        }
        directRdmaTransport_ = device::createIbgdaDeviceTransport(rdma_filters);
        if (directRdmaTransport_) {
            debug_log("rdma_initialize_begin");
            int rc = directRdmaTransport_->initialize("", meta_->activeSize,
                                                      num_qps);
            if (debug) {
                LOG(INFO) << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                          << " stage=initialize_done rc=" << rc;
            }
            if (rc == 0) {
                debug_log("rdma_register_memory_begin");
                rc = directRdmaTransport_->registerMemory(recv_buffer_[0],
                                                          kBufferSize);
                if (debug) {
                    LOG(INFO)
                        << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                        << " stage=register_memory_done rc=" << rc;
                }
            }
            if (rc == 0) {
                debug_log("rdma_allocate_control_begin");
                rc = directRdmaTransport_->allocateControlBuffer();
                if (debug) {
                    LOG(INFO)
                        << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                        << " stage=allocate_control_done rc=" << rc;
                }
            }
            if (rc == 0) {
                auto stream = at::cuda::getCurrentCUDAStream();
                debug_log("rdma_create_qp_begin");
                rc = directRdmaTransport_->createQueuePairs(stream.stream());
                if (debug) {
                    LOG(INFO)
                        << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                        << " stage=create_qp_done rc=" << rc;
                }
            }
            if (rc == 0) {
                auto local_meta = directRdmaTransport_->localMetadata();
                meta_->store->set(
                    directRdmaMetadataKey(meta_->backendIndex, rank_),
                    serializeRdmaMetadata(local_meta));
                debug_log("rdma_metadata_published");

                std::vector<std::string> rdma_keys;
                rdma_keys.reserve(meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    rdma_keys.push_back(
                        directRdmaMetadataKey(meta_->backendIndex, j));
                }
                BackoffWaiter rdma_waiter(BackoffWaiterConfig::constantSleep(
                    std::chrono::milliseconds(10)));
                debug_log("rdma_metadata_wait_begin");
                rdma_waiter.wait([&] { return meta_->store->check(rdma_keys); });
                debug_log("rdma_metadata_wait_done");

                std::vector<device::RdmaLocalMetadata> remote_meta(
                    meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    remote_meta[j] =
                        deserializeRdmaMetadata(meta_->store->get(rdma_keys[j]));
                    TORCH_CHECK(
                        remote_meta[j].qpns.size() >=
                            static_cast<size_t>(num_qps),
                        "Mooncake PG Device API RDMA metadata missing QPNs.");
                    TORCH_CHECK(
                        remote_meta[j].lids.size() >=
                            static_cast<size_t>(num_qps),
                        "Mooncake PG Device API RDMA metadata missing LIDs.");
                }
                debug_log("rdma_metadata_read_done");

                std::vector<int64_t> remote_addrs(meta_->activeSize, 0);
                std::vector<int32_t> remote_keys(meta_->activeSize, 0);
                std::vector<int64_t> subnet_prefixes(meta_->activeSize, 0);
                std::vector<int64_t> interface_ids(meta_->activeSize, 0);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    remote_addrs[j] = remote_meta[j].raddr;
                    remote_keys[j] = remote_meta[j].rkey;
                    subnet_prefixes[j] = remote_meta[j].subnet_prefix;
                    interface_ids[j] = remote_meta[j].interface_id;
                }

                std::vector<int32_t> remote_qpns(num_qps, 0);
                std::vector<int32_t> remote_lids(num_qps, 0);
                for (int i = 0; i < num_qps; ++i) {
                    const int peer_rank = i * meta_->activeSize / num_qps;
                    const int channel = i % directRdmaQpsPerRank_;
                    const int peer_local_qp =
                        rank_ * directRdmaQpsPerRank_ + channel;
                    remote_qpns[i] = remote_meta[peer_rank].qpns[peer_local_qp];
                    remote_lids[i] = remote_meta[peer_rank].lids[peer_local_qp];
                    if (debug) {
                        LOG(INFO)
                            << "MOONCAKE_PG_DEVICE_API_RDMA_QP_MAP rank="
                            << rank_ << " backend_index="
                            << meta_->backendIndex << " local_qp_idx=" << i
                            << " peer_rank=" << peer_rank
                            << " peer_local_qp=" << peer_local_qp
                            << " remote_qpn=" << remote_qpns[i]
                            << " remote_lid=" << remote_lids[i];
                    }
                }
                std::vector<int> active_mask(meta_->activeSize, 1);
                debug_log("rdma_connect_begin");
                rc = directRdmaTransport_->connectPeers(
                    rank_, directRdmaTransport_->isRoce(), remote_addrs,
                    remote_keys, remote_qpns, remote_lids, subnet_prefixes,
                    interface_ids, active_mask);
                if (debug) {
                    LOG(INFO)
                        << "MOONCAKE_PG_DEVICE_API_RDMA debug rank=" << rank_
                        << " stage=rdma_connect_done rc=" << rc;
                }
            }
            directRdmaReady_ = (rc == 0);
            if (!directRdmaReady_) {
                LOG(WARNING)
                    << "MOONCAKE_PG_DEVICE_API_RDMA init failed rank=" << rank_
                    << " rc=" << rc << "; cross-node Device API path disabled.";
                directRdmaTransport_.reset();
            }
        } else {
            debug_log("rdma_create_transport_failed");
        }
    }

    if (directRdmaReady_) {
        debug_log("shm_skipped_rdma");
    } else {
        const size_t signal_bytes =
            static_cast<size_t>(meta_->activeSize) * 2 * sizeof(uint32_t);
        std::string shm_name;
        const std::string shm_name_key =
            directP2pAllgatherShmNameKey(meta_->backendIndex);
        const std::string shm_ready_key =
            directP2pAllgatherShmReadyKey(meta_->backendIndex);
        if (rank_ == 0) {
            shm_name = "/mooncake_pg_direct_ag_" +
                       std::to_string(meta_->backendIndex) + "_" +
                       std::to_string(getpid());
            meta_->store->set(shm_name_key, shm_name);
            debug_log("shm_name_published");
        } else {
            BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                std::chrono::milliseconds(10)));
            debug_log("shm_name_wait_begin");
            waiter.wait([&] { return meta_->store->check({shm_name_key}); });
            auto shm_name_data = meta_->store->get(shm_name_key);
            shm_name.assign(shm_name_data.begin(), shm_name_data.end());
            debug_log("shm_name_wait_done");
        }

        int shm_fd = -1;
        if (rank_ == 0) {
            shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0600);
            TORCH_CHECK(shm_fd >= 0, "Failed to create direct P2P shm region.");
            TORCH_CHECK(ftruncate(shm_fd, signal_bytes) == 0,
                        "Failed to size direct P2P shm region.");
            debug_log("shm_create_done");
        } else {
            BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                std::chrono::milliseconds(10)));
            debug_log("shm_open_wait_begin");
            waiter.wait([&] {
                shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0600);
                return shm_fd >= 0;
            });
            debug_log("shm_open_wait_done");
        }
        void* mapped = mmap(nullptr, signal_bytes, PROT_READ | PROT_WRITE,
                            MAP_SHARED, shm_fd, 0);
        TORCH_CHECK(mapped != MAP_FAILED,
                    "Failed to mmap direct P2P shm signal region.");
        close(shm_fd);
        directP2pHostSignals_ = static_cast<uint32_t*>(mapped);
        directP2pHostSignalsBytes_ = signal_bytes;
        if (rank_ == 0) {
            std::memset(directP2pHostSignals_, 0, signal_bytes);
            meta_->store->set(shm_ready_key, "1");
            debug_log("shm_ready_published");
        } else {
            BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                std::chrono::milliseconds(10)));
            debug_log("shm_ready_wait_begin");
            waiter.wait([&] { return meta_->store->check({shm_ready_key}); });
            debug_log("shm_ready_wait_done");
        }
    }

    if (useDirectP2pDeviceSequence()) {
        const uint32_t initial_sequence = 1;
        debug_log("device_sequence_alloc_begin");
        cudaError_t seq_counter_err = cudaMalloc(
            &directP2pDeviceSequenceCounter_, sizeof(uint32_t));
        TORCH_CHECK(seq_counter_err == cudaSuccess,
                    "Failed to allocate direct P2P device sequence counter: ",
                    cudaGetErrorString(seq_counter_err));
        cudaError_t seq_counter_copy_err = cudaMemcpy(
            directP2pDeviceSequenceCounter_, &initial_sequence, sizeof(uint32_t),
            cudaMemcpyHostToDevice);
        TORCH_CHECK(seq_counter_copy_err == cudaSuccess,
                    "Failed to initialize direct P2P device sequence counter: ",
                    cudaGetErrorString(seq_counter_copy_err));
        const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
        cudaError_t seq_slots_err = cudaMalloc(
            &directP2pDeviceSequenceSlots_,
            static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
        TORCH_CHECK(seq_slots_err == cudaSuccess,
                    "Failed to allocate direct P2P device sequence slots: ",
                    cudaGetErrorString(seq_slots_err));
        cudaError_t seq_slots_memset_err = cudaMemset(
            directP2pDeviceSequenceSlots_, 0,
            static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
        TORCH_CHECK(seq_slots_memset_err == cudaSuccess,
                    "Failed to clear direct P2P device sequence slots: ",
                    cudaGetErrorString(seq_slots_memset_err));
        directP2pDeviceSequenceSlotCursor_ = 0;
        debug_log("device_sequence_alloc_done");
    }
    LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG ready="
              << directP2pAllgatherReady_ << " rank=" << rank_
              << " backend_index=" << meta_->backendIndex
              << " size=" << meta_->activeSize
              << " device_sequence=" << useDirectP2pDeviceSequence()
              << " device_api_rdma=" << directRdmaReady_;
#endif
}

void MooncakeBackend::setLocalOnlyActiveRanks() {
    for (int i = 0; i < meta_->size; ++i) {
        meta_->activeRanks[i] = (i == meta_->rank);
    }
    syncActiveRanksTensor();
}

void MooncakeBackend::waitForExtensionState() {
    TORCH_CHECK(meta_->store, "Recovery join requires a valid Store.");

    auto state_key = ConnectionContext::getExtensionStateStoreKey(
        meta_->backendIndex, rank_);

    BackoffWaiter waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(50)));

    waiter.wait([&] { return meta_->store->check({state_key}); });

    auto state_data = meta_->store->get(state_key);
    auto state = deserialize(state_data);

    // taskCount
    meta_->taskCount = state.taskCount;

    // p2pEpochs
    TORCH_CHECK(static_cast<size_t>(meta_->size) == state.p2pEpochs.size(),
                "Invalid p2pEpochs size");
    for (int i = 0; i < meta_->size; ++i) {
        p2p_proxy_->setEpoch(i, state.p2pEpochs[i]);
    }

    // activeRanks
    TORCH_CHECK(static_cast<size_t>(meta_->size) == state.activeRanks.size(),
                "Invalid activeRanks");
    for (int i = 0; i < meta_->size; ++i) {
        meta_->activeRanks[i] = state.activeRanks[i];
    }
    syncActiveRanksTensor();

    // activeSize: count the number of active ranks (contiguous from 0)
    int newActiveSize = 0;
    for (int i = 0; i < meta_->size; ++i) {
        if (meta_->activeRanks[i]) {
            newActiveSize = i + 1;
        }
    }
    meta_->activeSize = newActiveSize;
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
    const int oldActiveSize = meta_->activeSize;
    if (newSize == oldSize) return;

    TORCH_CHECK(newSize >= 0 && static_cast<size_t>(newSize) < kMaxNumRanks,
                "Size out of range");
    TORCH_CHECK(newSize >= oldSize, "newSize < oldSize");

    LOG(INFO) << "Backend " << backendIndex_ << " rank " << rank_
              << ": Group size extend to " << newSize;

    meta_->size = newSize;
    meta_->activeSize = newSize;
    meta_->taskCount = 0;

    // Initialize new rank's metadata
    for (int i = oldSize; i < newSize; ++i) {
        local2global_rank_map_[i] = i;
        // IMPORTANT: Newly-extended ranks must start as inactive.
        // They will only participate in collectives after healthy ranks
        // explicitly activate them via recoverRanks(). This enables a
        // two-phase scale-up protocol (extend capacity -> poll readiness
        // -> recover/activate) and avoids collectives including ranks that
        // haven't joined yet.
        meta_->activeRanks[i] = false;
    }

    auto& tensor = meta_->activeRanksTensor;
    if (newSize > tensor.numel()) {
        tensor.resize_({newSize});
    }
    tensor.slice(0, oldSize, newSize).fill_(0);

    connection_ctx_->extendGroupSizeTo(newSize);
    p2p_proxy_->extendGroupSizeTo(newSize);
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

    // Expand activeSize if any recovered rank is beyond the current boundary.
    if (!ranks.empty()) {
        const int max_rank = *std::max_element(ranks.begin(), ranks.end());
        if (max_rank >= meta_->activeSize) {
            meta_->activeSize = max_rank + 1;
        }
    }

    syncActiveRanksTensor();
    std::vector<uint32_t> epochs(meta_->size);
    for (int i = 0; i < meta_->size; ++i) {
        epochs[i] = p2p_proxy_->getEpoch(i);
    }
    ExtensionState state{
        .activeRanks =
            std::vector(meta_->activeRanks, meta_->activeRanks + meta_->size),
        .p2pEpochs = std::move(epochs),
        .taskCount = meta_->taskCount};
    auto state_data = serialize(state);
    for (const int rank : ranks) {
        auto key = ConnectionContext::getExtensionStateStoreKey(
            meta_->backendIndex, rank);
        meta_->store->set(key, state_data);
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

void MooncakeBackend::setExternalEngine(TransferEngine* engine) {
    externalEngine_ = engine;
    if (engine) {
        LOG(INFO) << "MooncakeBackend: external TransferEngine set (ptr="
                  << engine << ")";
    }
}
}  // namespace mooncake
