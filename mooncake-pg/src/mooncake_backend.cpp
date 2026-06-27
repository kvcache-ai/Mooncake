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
#include "connection_poller.h"
#include "memory_location.h"
#include "mooncake_worker.cuh"
#include "pg_utils.h"
#include <transport/device/device_transport.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

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

static bool useDirectP2pAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AG");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool directP2pAllgatherDebug() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AG_DEBUG");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDeviceRingAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] != '\0' && value[0] != '0';
}

static int deviceRingSignalMode() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    if (value && value[0] == '2') return 1;  // release/acquire PTX
    if (value && value[0] == '4') return 2;  // NCCL-like relaxed store + volatile load
    return 0;                                // CUDA system atomics
}

static bool useDeviceFifoRingAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] == '3';
}

static bool useDeviceStoreSignalAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] == '5';
}

static bool useDeviceStoreSignalSlottedAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_RING_AG");
    return value && value[0] == '6';
}

static int deviceStoreSignalSlots() {
    const char* value = std::getenv("MOONCAKE_PG_STORE_SIGNAL_SLOTS");
    if (!value || value[0] == '\0') return 8;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed < 2) return 2;
    if (parsed > 64) return 64;
    return static_cast<int>(parsed);
}

static size_t directP2pEffectiveBufferSize() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_BUFFER_MB");
    if (!value || value[0] == '\0') return kDefaultBufferSize;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return kDefaultBufferSize;
    const size_t mb = static_cast<size_t>(parsed);
    const size_t max_mb = kBufferSize >> 20;
    const size_t clamped_mb = std::min(std::max<size_t>(mb, 16), max_mb);
    return clamped_mb << 20;
}

static size_t directP2pSlotStride(size_t effectiveBufferSize,
                                  size_t signalBytes, int slots,
                                  int activeSize) {
    if (slots <= 0 || activeSize <= 0 || effectiveBufferSize <= signalBytes) {
        return 0;
    }
    return (((effectiveBufferSize - signalBytes) / static_cast<size_t>(slots) /
             static_cast<size_t>(activeSize)) &
            ~static_cast<size_t>(7));
}

static bool useDirectP2pCurrentStream() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_CURRENT_STREAM");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pNoEventWork() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_NO_EVENT");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pOutputAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_OUTPUT_AG");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pAllReduceDirectOutputAllgatherPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AR_DIRECT_OUTPUT_AG");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pReduceScatterPoc() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_RS");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pAllReducePoc() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AR");
    return value && value[0] != '\0' && value[0] != '0';
}

static size_t directP2pAllReduceMaxBytes() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_AR_MAX_BYTES");
    if (!value || value[0] == '\0') return 256 * 1024;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return 0;
    return static_cast<size_t>(parsed);
}

static bool directP2pCountEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_COUNT");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool directP2pAllReduceStageProfileEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_AR_STAGE_PROFILE");
    return value && value[0] != '\0' && value[0] != '0';
}

static uint64_t directP2pAllReduceStageProfileLimit() {
    const char* value = std::getenv("MOONCAKE_PG_AR_STAGE_PROFILE_LIMIT");
    if (!value || value[0] == '\0') return 8;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value || parsed == 0) return 8;
    return parsed;
}

static bool useDirectP2pChunked() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_CHUNKED");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDirectP2pDeviceSequence() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useFusedDirectP2pDeviceSequenceReserve() {
    const char* value = std::getenv("MOONCAKE_PG_FUSED_DEVICE_SEQUENCE");
    return value && value[0] != '\0' && value[0] != '0';
}

static uint32_t directP2pDeviceSequenceSlotCapacity() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_DEVICE_SEQUENCE_SLOTS");
    if (!value || value[0] == '\0') return 1u << 20;
    char* end = nullptr;
    unsigned long parsed = std::strtoul(value, &end, 10);
    if (end == value || parsed < 128) return 128;
    if (parsed > 1u << 20) return 1u << 20;
    return static_cast<uint32_t>(parsed);
}

static bool disableDirectP2pDuringCudaGraphCapture() {
    const char* value = std::getenv("MOONCAKE_PG_DIRECT_P2P_GRAPH_CAPTURE");
    if (useDirectP2pDeviceSequence()) return false;
    // The slotted direct-P2P protocol currently embeds host-generated sequence
    // numbers into captured kernels. CUDA graph replay reuses those constants,
    // which is unsafe for producer/consumer signals across graph replays. Keep
    // graph capture correct by defaulting captured regions to the existing
    // Mooncake PG path until a runtime device-sequence protocol is added.
    return !value || value[0] == '\0' || value[0] == '0';
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
            int deviceId_;
            err = cudaGetDevice(&deviceId_);
            TORCH_CHECK(!err, c10::str("Failed to get device id"));
            location = GPU_PREFIX + std::to_string(deviceId_);
        }
    }

    // Initialize transfer engine
    if (externalEngine_) {
        // Use externally-provided engine (already initialized), skip init.
        engine_ = externalEngine_;
        engineInitialized_ = true;
    } else if (!engineInitialized_) {
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
                     .size = size_,
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
        maybeInitDirectP2pAllgather();
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
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            isCudaStreamCapturing(stream.stream());
        const int store_signal_slots = deviceStoreSignalSlots();
        const size_t direct_buffer_size = directP2pEffectiveBufferSize();
        const size_t direct_signal_bytes =
            static_cast<size_t>(store_signal_slots) * kMaxNumRanks *
            sizeof(uint32_t) * 2;
        const size_t direct_slot_stride = directP2pSlotStride(
            direct_buffer_size, direct_signal_bytes, store_signal_slots,
            meta_->activeSize);
        const bool use_device_sequence = useDirectP2pDeviceSequence() &&
            directP2pDeviceSequenceCounter_ && directP2pDeviceSequenceSlots_;
        const size_t direct_ar_segment_bytes =
            meta_->activeSize > 0
                ? tensorSize / static_cast<size_t>(meta_->activeSize)
                : 0;
        const bool can_direct_ar_rsag = !direct_p2p_capture_disabled &&
            useDirectP2pAllReducePoc() && useDirectP2pReduceScatterPoc() &&
            useDirectP2pAllgatherPoc() && useDirectP2pChunked() &&
            directP2pAllgatherReady_ && tensor.is_contiguous() &&
            meta_->activeSize == meta_->size &&
            opts.reduceOp == c10d::ReduceOp::SUM && meta_->activeSize > 0 &&
            tensorSize % static_cast<size_t>(meta_->activeSize) == 0 &&
            direct_ar_segment_bytes % tensor.element_size() == 0 &&
            direct_slot_stride > 0;
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
            const size_t direct_ag_sequence_count =
                want_ar_direct_output_ag ? 1 : direct_ag_chunk_count;
            const bool fuse_device_sequence_reserve =
                use_device_sequence && useFusedDirectP2pDeviceSequenceReserve() &&
                !use_chunked_ar && !want_ar_direct_output_ag;
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

            if (use_chunked_ar && use_device_sequence) {
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

            if (ar_output_cache) {
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
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            isCudaStreamCapturing(stream.stream());
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
            ? static_cast<size_t>(store_signal_slots) * kMaxNumRanks *
                  sizeof(uint32_t) * 2
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
        if (!direct_p2p_capture_disabled && directP2pAllgatherReady_ &&
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
            } else {
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            }
            if (directP2pAllgatherDebug()) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG_DEBUG rank=" << rank_
                          << " seq=" << sequence
                          << " stage=device_collective_enqueue";
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
                launchP2pAllgatherStoreSignalSlottedChunkedGraphKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, device_sequence_slot,
                    enq_stream.stream());
            } else if (use_chunked_slotted) {
                launchP2pAllgatherStoreSignalSlottedChunkedKernel(
                    inputBuffer.data_ptr(), outputBuffer.data_ptr(),
                    recv_buffer_[0], directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, sequence, enq_stream.stream());
            } else if (use_store_signal_slotted && use_device_sequence) {
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
        const bool direct_p2p_capture_disabled =
            disableDirectP2pDuringCudaGraphCapture() &&
            isCudaStreamCapturing(stream.stream());
        const int store_signal_slots = deviceStoreSignalSlots();
        const size_t direct_buffer_size = directP2pEffectiveBufferSize();
        const size_t direct_signal_bytes =
            static_cast<size_t>(store_signal_slots) * kMaxNumRanks *
            sizeof(uint32_t) * 2;
        const size_t direct_slot_stride = directP2pSlotStride(
            direct_buffer_size, direct_signal_bytes, store_signal_slots,
            meta_->activeSize);
        const bool use_chunked_rs = useDirectP2pChunked() &&
            tensorSize > direct_slot_stride && direct_slot_stride > 0;
        const size_t direct_chunk_bytes = use_chunked_rs
            ? ((direct_slot_stride / outputBuffer.element_size()) *
               outputBuffer.element_size())
            : tensorSize;
        const size_t direct_chunk_count = use_chunked_rs
            ? ((tensorSize + direct_chunk_bytes - 1) / direct_chunk_bytes)
            : 1;
        const bool use_device_sequence = useDirectP2pDeviceSequence() &&
            directP2pDeviceSequenceCounter_ && directP2pDeviceSequenceSlots_;
        if (!direct_p2p_capture_disabled && useDirectP2pReduceScatterPoc() &&
            directP2pAllgatherReady_ &&
            inputBuffer.is_contiguous() && outputBuffer.is_contiguous() &&
            meta_->activeSize == meta_->size && opts.reduceOp == c10d::ReduceOp::SUM &&
            (tensorSize <= direct_slot_stride || use_chunked_rs) &&
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
            } else {
                directP2pAllgatherSequence_ +=
                    static_cast<uint32_t>(direct_chunk_count);
            }
            if (use_chunked_rs && use_device_sequence) {
                launchP2pReduceScatterSlottedChunkedGraphKernel(
                    outputBuffer, inputBuffer, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, device_sequence_slot,
                    stream.stream());
            } else if (use_chunked_rs) {
                launchP2pReduceScatterSlottedChunkedKernel(
                    outputBuffer, inputBuffer, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_chunk_bytes, direct_slot_stride, store_signal_slots,
                    rank_, meta_->activeSize, sequence, stream.stream());
            } else if (use_device_sequence) {
                launchP2pReduceScatterSlottedGraphKernel(
                    outputBuffer, inputBuffer, recv_buffer_[0],
                    directP2pTransport_->peerPtrsTablePtr(),
                    directP2pTransport_->availableTablePtr(), tensorSize,
                    direct_slot_stride, store_signal_slots, rank_, meta_->activeSize,
                    device_sequence_slot,
                    fuse_device_sequence_reserve
                        ? directP2pDeviceSequenceCounter_
                        : nullptr,
                    static_cast<uint32_t>(direct_chunk_count), stream.stream());
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
            if (shouldLogDirectP2pCount(g_directP2pRsFastCount)) {
                LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_COUNT op=reduce_scatter_base "
                          << "path=fast rank=" << rank_
                          << " size=" << meta_->activeSize
                          << " bytes=" << tensorSize
                          << " seq=" << sequence
                          << " buffer_bytes=" << direct_buffer_size
                          << " device_sequence=" << use_device_sequence
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
                      << " input_contiguous=" << inputBuffer.is_contiguous()
                      << " output_contiguous=" << outputBuffer.is_contiguous()
                      << " capture_disabled=" << direct_p2p_capture_disabled
                      << " full_active="
                      << (meta_->activeSize == meta_->size)
                      << " sum=" << (opts.reduceOp == c10d::ReduceOp::SUM)
                      << " buffer_bytes=" << direct_buffer_size
                      << " slot_stride=" << direct_slot_stride
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

void MooncakeBackend::maybeInitDirectP2pAllgather() {
#ifdef MOONCAKE_EP_USE_MUSA
    return;
#else
    if (isCpu_ || !useDirectP2pAllgatherPoc()) return;
    TORCH_CHECK(meta_->store,
                "Direct P2P allgather PoC requires a valid Store.");
    TORCH_CHECK(static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
                "Direct P2P allgather active size exceeds rank limit.");

    cudaError_t memset_err = cudaMemset(recv_buffer_[0], 0, kBufferSize);
    TORCH_CHECK(memset_err == cudaSuccess,
                "Failed to clear direct P2P allgather buffer: ",
                cudaGetErrorString(memset_err));

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

    std::vector<std::string> keys;
    keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        keys.push_back(directP2pAllgatherKey(meta_->backendIndex, j));
    }
    BackoffWaiter waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
    waiter.wait([&] { return meta_->store->check(keys); });

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
    directP2pTransport_->importPeerHandles(
        recv_buffer_[0], rank_, meta_->activeSize, remote_handles, active_mask);
    directP2pAllgatherReady_ = directP2pTransport_->allPeersAccessible();
    directP2pPeerPtrsHost_.assign(meta_->activeSize, nullptr);
    cudaError_t peer_copy_err = cudaMemcpy(
        directP2pPeerPtrsHost_.data(), directP2pTransport_->peerPtrsTablePtr(),
        directP2pPeerPtrsHost_.size() * sizeof(void*), cudaMemcpyDeviceToHost);
    TORCH_CHECK(peer_copy_err == cudaSuccess,
                "Failed to copy direct P2P peer pointer table: ",
                cudaGetErrorString(peer_copy_err));

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
    } else {
        BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
            std::chrono::milliseconds(10)));
        waiter.wait([&] { return meta_->store->check({shm_name_key}); });
        auto shm_name_data = meta_->store->get(shm_name_key);
        shm_name.assign(shm_name_data.begin(), shm_name_data.end());
    }

    int shm_fd = -1;
    if (rank_ == 0) {
        shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0600);
        TORCH_CHECK(shm_fd >= 0, "Failed to create direct P2P shm region.");
        TORCH_CHECK(ftruncate(shm_fd, signal_bytes) == 0,
                    "Failed to size direct P2P shm region.");
    } else {
        BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
            std::chrono::milliseconds(10)));
        waiter.wait([&] {
            shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0600);
            return shm_fd >= 0;
        });
    }
    void* mapped =
        mmap(nullptr, signal_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    TORCH_CHECK(mapped != MAP_FAILED,
                "Failed to mmap direct P2P shm signal region.");
    close(shm_fd);
    directP2pHostSignals_ = static_cast<uint32_t*>(mapped);
    directP2pHostSignalsBytes_ = signal_bytes;
    if (rank_ == 0) {
        std::memset(directP2pHostSignals_, 0, signal_bytes);
        meta_->store->set(shm_ready_key, "1");
    } else {
        BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
            std::chrono::milliseconds(10)));
        waiter.wait([&] { return meta_->store->check({shm_ready_key}); });
    }

    if (useDirectP2pDeviceSequence()) {
        const uint32_t initial_sequence = 1;
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
    }
    LOG(INFO) << "MOONCAKE_PG_DIRECT_P2P_AG ready="
              << directP2pAllgatherReady_ << " rank=" << rank_
              << " size=" << meta_->activeSize
              << " device_sequence=" << useDirectP2pDeviceSequence();
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
