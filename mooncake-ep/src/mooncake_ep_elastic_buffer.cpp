#include <elastic/mooncake_ep_elastic_buffer.h>
#include <elastic/mooncake_ep_elastic_launch.cuh>
#include <elastic/mooncake_ep_elastic_layout.cuh>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <stdexcept>

#include <cuda_alike.h>
#include <glog/logging.h>
#ifdef USE_NCCL_DEVICE
#include <transport/device/nccl_device_transport.h>
#endif

namespace mooncake {
namespace {

int64_t ceil_div_i64(int64_t x, int64_t y) { return (x + y - 1) / y; }

constexpr int kIbgdaElasticHybridChannelsPerSm = 4;
constexpr int kNcclElasticHybridChannelsPerSm = 8;

int64_t align_i64(int64_t x, int64_t alignment) {
    return ceil_div_i64(x, alignment) * alignment;
}

int getenv_int(const char* name, int default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') return default_value;
    return std::max(1, std::atoi(value));
}

int hybrid_num_channels(int num_sms, int channels_per_sm) {
    return std::max(1, num_sms) * channels_per_sm;
}

int hybrid_num_max_tokens_per_channel(int num_max_tokens_per_rank, int num_sms,
                                      int channels_per_sm) {
    return static_cast<int>(
        ceil_div_i64(num_max_tokens_per_rank,
                     hybrid_num_channels(num_sms, channels_per_sm)));
}

int64_t elastic_workspace_num_bytes() {
    // Preserve the established host reservation and payload offset.
    constexpr int64_t kNumMaxRanks = 1024;
    constexpr int64_t kNumMaxExperts = 2048;
    constexpr int64_t kNumMaxChannels = 8 * 160;
    constexpr int64_t kNumMaxInflightAGRS = 32;
    constexpr int64_t kNumBarrierTags = 16;

    int64_t num_bytes = 0;
    num_bytes += kNumBarrierTags *
                 (sizeof(unsigned long long) + 2 * kNumMaxRanks * sizeof(int));
    num_bytes += (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t);
    num_bytes += kNumMaxRanks * sizeof(int64_t) * 2;
    num_bytes += kNumMaxExperts * sizeof(int64_t) * 2;
    num_bytes += kNumMaxRanks * sizeof(int);
    num_bytes += kNumMaxRanks * sizeof(int) * 2;
    num_bytes += kNumMaxExperts * sizeof(int) * 2;
    num_bytes += kNumMaxRanks * kNumMaxChannels * sizeof(int64_t);
    num_bytes += kNumMaxRanks * kNumMaxChannels * sizeof(int);
    num_bytes += 2 * 2 * sizeof(int64_t);
    num_bytes += (kNumMaxInflightAGRS + 1) * kNumMaxRanks * sizeof(int);
    return align_i64(num_bytes, 32);
}

int64_t elastic_atomic_scratch_num_bytes() {
    return elastic_workspace_num_bytes();
}

#ifdef USE_NCCL_DEVICE
class NcclCommStream {
   public:
    NcclCommStream() {
        int least_priority = 0;
        int greatest_priority = 0;
        auto status = cudaDeviceGetStreamPriorityRange(&least_priority,
                                                       &greatest_priority);
        if (status != cudaSuccess) {
            cudaGetLastError();
            greatest_priority = 0;
        }
        CUDA_CHECK(cudaStreamCreateWithPriority(
            &stream_, cudaStreamNonBlocking, greatest_priority));
    }

    NcclCommStream(const NcclCommStream&) = delete;
    NcclCommStream& operator=(const NcclCommStream&) = delete;

    ~NcclCommStream() { reset(); }

    cudaStream_t stream() const { return stream_; }

    cudaError_t reset() {
        if (stream_ == nullptr) return cudaSuccess;
        const auto status = cudaStreamDestroy(stream_);
        stream_ = nullptr;
        return status;
    }

   private:
    cudaStream_t stream_ = nullptr;
};

int nccl_gin_context_count(int requested_count, bool allow_hybrid_mode) {
    // Match upstream DeepEP v2: hybrid mode reserves one notify context plus
    // 64 data contexts; direct mode reserves one notify plus 16 data contexts.
    const int default_count = allow_hybrid_mode ? 65 : 17;
    constexpr int kMaxGinContexts = MAX_QP_COUNT;
    const int count =
        requested_count > 0
            ? requested_count
            : getenv_int("MOONCAKE_EP_NCCL_GIN_CONTEXTS", default_count);
    if (count > kMaxGinContexts) {
        throw std::invalid_argument(
            "NCCL ElasticBuffer GIN context count exceeds MAX_QP_COUNT");
    }
    return count;
}
#endif
}  // namespace

struct NcclElasticState {
#ifdef USE_NCCL_DEVICE
    std::unique_ptr<device::NcclTransport> transport;
    device::NcclBufferRegistration registration;
    device::NcclDeviceContext device_context;
    device::NcclTransportProperties properties;
    device::NcclLsaTopology lsa_topology;
    void* allocation = nullptr;
    size_t allocation_bytes = 0;
    int device_id = -1;
    int clock_rate_khz = 0;
    NcclCommStream comm_stream;

    NcclElasticState(int rank, int num_ranks, size_t bytes,
                     int gin_context_count, bool use_rail_gin,
                     int gin_traffic_class,
                     const std::vector<int32_t>& nccl_unique_id)
        : transport(device::createNcclDeviceTransport()),
          allocation_bytes(bytes) {
        if (!transport) {
            throw std::runtime_error(
                "failed to create the NCCL device transport");
        }

        CUDA_CHECK(cudaGetDevice(&device_id));
        CUDA_CHECK(cudaDeviceGetAttribute(&clock_rate_khz, cudaDevAttrClockRate,
                                          device_id));

        device::NcclTransportConfig config;
        config.rank = rank;
        config.num_ranks = num_ranks;
        config.enable_gin = num_ranks > 1;
        config.gin_connection_type = use_rail_gin
                                         ? device::NcclGinConnectionType::kRail
                                         : device::NcclGinConnectionType::kFull;
        config.gin_context_count = config.enable_gin ? gin_context_count : 0;
        config.gin_exclusive_contexts = config.enable_gin;
        config.gin_queue_depth = config.enable_gin ? 1024 : 0;
        config.gin_signal_count = config.enable_gin ? num_ranks + 4 : 0;
        config.gin_traffic_class = gin_traffic_class;
        config.lsa_barrier_count = 0;
        config.require_lsa_multimem = false;
        if (transport->initialize(config, nccl_unique_id) != 0) {
            throw std::runtime_error(
                "failed to initialize the NCCL ElasticBuffer transport");
        }

        properties = transport->properties();
        lsa_topology = transport->lsaTopology();
        if (transport->allocateAndRegisterBuffer(allocation_bytes, &allocation,
                                                 &registration) != 0) {
            throw std::runtime_error(
                "failed to allocate and register the NCCL ElasticBuffer");
        }
        device_context = transport->deviceContext(registration);
        if (!transport->allRanksSucceeded(device_context.valid())) {
            throw std::runtime_error(
                "failed to create the NCCL ElasticBuffer device context on "
                "one or more ranks");
        }
    }

    int release() noexcept {
        if (!transport) return 0;

        int status = 0;
        int previous_device = -1;
        if (cudaGetDevice(&previous_device) != cudaSuccess ||
            cudaSetDevice(device_id) != cudaSuccess) {
            status = -1;
        }
        if (cudaStreamSynchronize(comm_stream.stream()) != cudaSuccess) {
            status = -1;
        }
        if (registration.valid() &&
            transport->deregisterBuffer(&registration) != 0) {
            status = -1;
        }
        if (allocation != nullptr && !registration.valid()) {
            if (transport->freeBuffer(allocation) != 0) status = -1;
        }
        if (transport->shutdown() != 0) status = -1;
        if (comm_stream.reset() != cudaSuccess) status = -1;

        allocation = nullptr;
        device_context = {};
        transport.reset();
        if (previous_device >= 0 && previous_device != device_id &&
            cudaSetDevice(previous_device) != cudaSuccess) {
            status = -1;
        }
        return status;
    }
#endif

    ~NcclElasticState() {
#ifdef USE_NCCL_DEVICE
        release();
#endif
    }
};

ElasticLaunchContext MooncakeElasticBuffer::make_launch_context(
    int64_t timeout_cycles) const {
    ElasticLaunchContext ctx;
    ctx.device_id = device_id_;
    const auto workspace_bytes = elastic_workspace_num_bytes();
    const auto scratch_bytes = elastic_atomic_scratch_num_bytes();

    char* local_base = nullptr;
    if (using_nccl()) {
#ifdef USE_NCCL_DEVICE
        local_base = static_cast<char*>(nccl_state_->allocation);
        ctx.backend = ElasticTransportBackend::kNccl;
        ctx.nccl.device = nccl_state_->device_context;
        ctx.num_qps = std::max(1, nccl_state_->properties.gin_context_count);
#else
        throw std::logic_error(
            "NCCL ElasticBuffer state exists in a non-NCCL build");
#endif
    } else {
        if (!native_buffer_) {
            throw std::logic_error(
                "ElasticBuffer transport has already been destroyed");
        }
        auto& buffer = *native_buffer_;
        auto* rdma = buffer.rdma_transport_;
        local_base = static_cast<char*>(buffer.gdr_buffer);
        ctx.backend = ElasticTransportBackend::kIbgda;
        ctx.nvlink_available = buffer.p2p_transport_->availableTablePtr();
        ctx.ipc_peer_ptrs = buffer.p2p_transport_->peerPtrsTablePtr();
        ctx.raddrs = rdma ? rdma->raddrsPtr() : nullptr;
        ctx.rkeys = rdma ? rdma->rkeysPtr() : nullptr;
        ctx.qp_devctxs = rdma ? rdma->qpDevCtxsPtr() : nullptr;
        ctx.rdma_send_signal_buffer = local_base + workspace_bytes;
        ctx.rdma_recv_signal_buffer = local_base;
        ctx.num_qps = buffer.USE_QP_COUNT;
    }

    // Both backends expose one registered allocation. Keep the established
    // workspace and IBGDA atomic-response prefix sizes for a common payload
    // offset; NCCL does not access the second prefix.
    ctx.gdr_buffer = local_base;
    ctx.workspace = local_base;
    ctx.buffer = local_base + workspace_bytes + scratch_bytes;
    ctx.mapped_host_workspace = mapped_host_workspace_;
    ctx.rank = topology_.rank_idx;
    ctx.num_ranks = topology_.num_ranks;
    ctx.scaleout_rank_idx = topology_.scaleout_rank_idx;
    ctx.scaleup_rank_idx = topology_.scaleup_rank_idx;
    ctx.num_scaleout_ranks = topology_.num_scaleout_ranks;
    ctx.num_scaleup_ranks = topology_.num_scaleup_ranks;
    ctx.is_scaleup_nvlink = topology_.scaleup_lsa;
    ctx.timeout_cycles = timeout_cycles;
    return ctx;
}

std::vector<int32_t> create_elastic_nccl_unique_id() {
#ifdef USE_NCCL_DEVICE
    auto transport = device::createNcclDeviceTransport();
    if (!transport) {
        throw std::runtime_error("failed to create the NCCL device transport");
    }
    auto unique_id = transport->createUniqueId();
    if (unique_id.empty()) {
        throw std::runtime_error("failed to create an NCCL unique ID");
    }
    return unique_id;
#else
    throw std::runtime_error(
        "Mooncake EP was built without NCCL Device API support; rebuild with "
        "MOONCAKE_EP_USE_NCCL_DEVICE=1");
#endif
}

MooncakeElasticBuffer::MooncakeElasticBuffer(
    int rank, int num_ranks, int64_t num_buffer_bytes,
    int64_t num_max_tokens_per_rank, int64_t hidden, int64_t num_topk,
    bool use_fp8_dispatch, bool deterministic, bool allow_hybrid_mode,
    bool allow_multiple_reduction, bool prefer_overlap_with_compute, int sl_idx,
    int num_allocated_qps, int num_cpu_timeout_secs, int num_gpu_timeout_secs)
    : MooncakeElasticBuffer(
          rank, num_ranks, num_buffer_bytes, num_max_tokens_per_rank, hidden,
          num_topk, use_fp8_dispatch, deterministic, allow_hybrid_mode,
          allow_multiple_reduction, prefer_overlap_with_compute, sl_idx,
          num_allocated_qps, num_cpu_timeout_secs, num_gpu_timeout_secs,
          "ibgda", {}) {}

MooncakeElasticBuffer::MooncakeElasticBuffer(
    int rank, int num_ranks, int64_t num_buffer_bytes,
    int64_t num_max_tokens_per_rank, int64_t hidden, int64_t num_topk,
    bool use_fp8_dispatch, bool deterministic, bool allow_hybrid_mode,
    bool allow_multiple_reduction, bool prefer_overlap_with_compute, int sl_idx,
    int num_allocated_qps, int num_cpu_timeout_secs, int num_gpu_timeout_secs,
    const std::string& transport, const std::vector<int32_t>& nccl_unique_id)
    : transport_(transport) {
    if (rank < 0 || num_ranks <= 0 || rank >= num_ranks) {
        throw std::invalid_argument("invalid ElasticBuffer rank or world size");
    }
    if (!allow_multiple_reduction) {
        throw std::runtime_error(
            "Mooncake ElasticBuffer currently supports only "
            "allow_multiple_reduction=true");
    }
    if (transport_ != "ibgda" && transport_ != "nccl") {
        throw std::invalid_argument(
            "ElasticBuffer transport must be either 'ibgda' or 'nccl'");
    }

    CUDA_CHECK(cudaGetDevice(&device_id_));
    CUDA_CHECK(cudaDeviceGetAttribute(
        &physical_num_sms_, cudaDevAttrMultiProcessorCount, device_id_));
#ifdef MOONCAKE_EP_USE_MUSA
    device_smem_bytes_ = 0;
#else
    CUDA_CHECK(cudaDeviceGetAttribute(&device_smem_bytes_,
                                      cudaDevAttrMaxSharedMemoryPerBlockOptin,
                                      device_id_));
    if (device_smem_bytes_ <= 0) device_smem_bytes_ = 98304;
#endif

    config_.num_max_tokens_per_rank = num_max_tokens_per_rank;
    config_.hidden = hidden;
    config_.num_topk = num_topk;
    config_.use_fp8_dispatch = use_fp8_dispatch;
    config_.deterministic = deterministic;
    config_.allow_hybrid_mode = allow_hybrid_mode;
    config_.allow_multiple_reduction = allow_multiple_reduction;
    config_.prefer_overlap_with_compute = prefer_overlap_with_compute;
    config_.sl_idx = sl_idx;
    config_.num_allocated_qps = num_allocated_qps;
    config_.num_cpu_timeout_secs = num_cpu_timeout_secs;
    config_.num_gpu_timeout_secs = num_gpu_timeout_secs;

    if (num_buffer_bytes == 0) {
        num_buffer_bytes = calculate_buffer_size(
            num_ranks, num_max_tokens_per_rank, hidden, num_topk,
            use_fp8_dispatch, allow_hybrid_mode, allow_multiple_reduction);
    }
    if (num_buffer_bytes <= 0) {
        throw std::invalid_argument("ElasticBuffer size must be positive");
    }

    if (transport_ == "nccl") {
#ifdef USE_NCCL_DEVICE
        const int context_count =
            nccl_gin_context_count(num_allocated_qps, allow_hybrid_mode);
        nccl_state_ = std::make_unique<NcclElasticState>(
            rank, num_ranks, static_cast<size_t>(num_buffer_bytes),
            context_count, allow_hybrid_mode, sl_idx, nccl_unique_id);
        const auto& properties = nccl_state_->properties;
        const auto& lsa = nccl_state_->lsa_topology;
        const bool local_lsa_topology_valid =
            properties.rank == rank && properties.num_ranks == num_ranks &&
            lsa.rank >= 0 && lsa.size > 0 && lsa.first_rank >= 0 &&
            lsa.first_rank + lsa.rank == rank &&
            lsa.first_rank + lsa.size <= num_ranks &&
            num_ranks % lsa.size == 0 &&
            lsa.first_rank == (rank / lsa.size) * lsa.size;
        if (!nccl_state_->transport->allRanksSucceeded(
                local_lsa_topology_valid)) {
            throw std::runtime_error(
                "NCCL LSA membership on one or more ranks is not a "
                "contiguous, equal-sized EP local team; reorder "
                "process-group ranks by node/device");
        }

        topology_.rank_idx = rank;
        topology_.num_ranks = num_ranks;
        topology_.num_rdma_ranks = num_ranks / lsa.size;
        topology_.num_nvlink_ranks = lsa.size;
        const bool local_mode_supported =
            topology_.num_rdma_ranks <= 1 || allow_hybrid_mode;
        if (!nccl_state_->transport->allRanksSucceeded(local_mode_supported)) {
            throw std::runtime_error(
                "multi-node NCCL ElasticBuffer requires "
                "allow_hybrid_mode=true; full-world GIN kernels are not part "
                "of the initial backend");
        }
        const bool local_topology_supported =
            topology_.num_rdma_ranks == 1
                ? (lsa.size == 2 || lsa.size == 8)
                : ((topology_.num_rdma_ranks == 2 &&
                    (lsa.size == 4 || lsa.size == 8)) ||
                   (topology_.num_rdma_ranks == 4 && lsa.size == 4));
        if (!nccl_state_->transport->allRanksSucceeded(
                local_topology_supported)) {
            throw std::runtime_error(
                "NCCL ElasticBuffer currently supports one LSA team of 2 or 8 "
                "GPUs, two LSA teams of 4 or 8 GPUs, or four LSA teams of 4 "
                "GPUs");
        }
        if (allow_hybrid_mode && topology_.num_rdma_ranks > 1) {
            topology_.num_scaleout_ranks = topology_.num_rdma_ranks;
            topology_.num_scaleup_ranks = topology_.num_nvlink_ranks;
            topology_.scaleout_rank_idx = lsa.first_rank / lsa.size;
            topology_.scaleup_rank_idx = lsa.rank;
            topology_.hybrid_enabled = true;
            topology_.scaleup_lsa = true;
        } else {
            topology_.num_scaleout_ranks = 1;
            topology_.num_scaleup_ranks = num_ranks;
            topology_.scaleout_rank_idx = 0;
            topology_.scaleup_rank_idx = rank;
            topology_.hybrid_enabled = false;
            topology_.scaleup_lsa = topology_.num_rdma_ranks == 1;
        }
#else
        (void)nccl_unique_id;
        throw std::runtime_error(
            "transport='nccl' requires a Mooncake EP build with "
            "MOONCAKE_EP_USE_NCCL_DEVICE=1");
#endif
    } else {
        if (!nccl_unique_id.empty()) {
            throw std::invalid_argument(
                "nccl_unique_id must be empty for transport='ibgda'");
        }
        topology_ = discover_topology(rank, num_ranks, allow_hybrid_mode);
        native_buffer_ = std::make_unique<MooncakeEpBuffer>(rank, num_ranks,
                                                            num_buffer_bytes);
    }

    host_workspace_bytes_ = elastic_workspace_num_bytes();
    if (using_nccl()) {
#ifdef USE_NCCL_DEVICE
        const cudaError_t allocation_status = cudaHostAlloc(
            &host_workspace_, host_workspace_bytes_, cudaHostAllocMapped);
        cudaError_t mapping_status = cudaSuccess;
        if (allocation_status == cudaSuccess) {
            mapping_status = cudaHostGetDevicePointer(&mapped_host_workspace_,
                                                      host_workspace_, 0);
        }
        const bool local_workspace_valid =
            allocation_status == cudaSuccess && mapping_status == cudaSuccess;
        if (!nccl_state_->transport->allRanksSucceeded(local_workspace_valid)) {
            if (host_workspace_ != nullptr) cudaFreeHost(host_workspace_);
            host_workspace_ = nullptr;
            mapped_host_workspace_ = nullptr;
            throw std::runtime_error(
                "failed to allocate and map the NCCL ElasticBuffer host "
                "workspace on one or more ranks");
        }
        std::memset(host_workspace_, 0, host_workspace_bytes_);
#else
        throw std::logic_error(
            "NCCL ElasticBuffer state exists in a non-NCCL build");
#endif
    } else {
        try {
            CUDA_CHECK(cudaHostAlloc(&host_workspace_, host_workspace_bytes_,
                                     cudaHostAllocMapped));
            CUDA_CHECK(cudaHostGetDevicePointer(&mapped_host_workspace_,
                                                host_workspace_, 0));
            std::memset(host_workspace_, 0, host_workspace_bytes_);
        } catch (...) {
            if (host_workspace_ != nullptr) cudaFreeHost(host_workspace_);
            host_workspace_ = nullptr;
            mapped_host_workspace_ = nullptr;
            throw;
        }
    }
}

MooncakeElasticBuffer::~MooncakeElasticBuffer() {
    try {
        destroy();
    } catch (const std::exception& error) {
        LOG(ERROR) << "ElasticBuffer cleanup failed: " << error.what();
    } catch (...) {
        LOG(ERROR) << "ElasticBuffer cleanup failed with an unknown error";
    }
}

void MooncakeElasticBuffer::destroy() {
    if (destroyed_) return;
    destroyed_ = true;

    std::exception_ptr cleanup_error;
#ifdef USE_NCCL_DEVICE
    if (nccl_state_) {
        if (nccl_state_->release() != 0) {
            cleanup_error = std::make_exception_ptr(
                std::runtime_error("NCCL ElasticBuffer cleanup failed"));
        }
        nccl_state_.reset();
    }
#endif
    if (native_buffer_) {
        auto* buffer = native_buffer_.release();
        if (cudaStreamSynchronize(buffer->comm_stream) != cudaSuccess &&
            !cleanup_error) {
            cleanup_error = std::make_exception_ptr(std::runtime_error(
                "failed to synchronize the IBGDA ElasticBuffer stream"));
        }
        try {
            delete buffer;
        } catch (...) {
            if (!cleanup_error) cleanup_error = std::current_exception();
        }
    }
    if (host_workspace_ != nullptr) {
        if (cudaFreeHost(host_workspace_) != cudaSuccess && !cleanup_error) {
            cleanup_error = std::make_exception_ptr(
                std::runtime_error("failed to free ElasticBuffer workspace"));
        }
        host_workspace_ = nullptr;
        mapped_host_workspace_ = nullptr;
    }
    if (cleanup_error) std::rethrow_exception(cleanup_error);
}

MooncakeEpBuffer& MooncakeElasticBuffer::native_buffer() {
    if (!native_buffer_) {
        throw std::runtime_error(
            "this ElasticBuffer uses NCCL or has already been destroyed");
    }
    return *native_buffer_;
}

bool MooncakeElasticBuffer::ibgda_disabled() const {
    if (using_nccl()) return true;
    if (!native_buffer_) {
        throw std::runtime_error("ElasticBuffer has already been destroyed");
    }
    return native_buffer_->ibgda_disabled();
}

bool MooncakeElasticBuffer::use_fast_path() {
    return using_nccl() ? true : native_buffer().use_fast_path();
}

void MooncakeElasticBuffer::update_local_qpns() {
    if (using_nccl()) {
        throw std::runtime_error(
            "NCCL ElasticBuffer membership is fixed; recreate the buffer "
            "instead of updating QPs");
    }
    native_buffer().update_local_qpns();
}

bool MooncakeElasticBuffer::is_roce() const {
    if (using_nccl()) return false;
    if (!native_buffer_) {
        throw std::runtime_error("ElasticBuffer has already been destroyed");
    }
    return native_buffer_->is_roce();
}

void MooncakeElasticBuffer::sync_ibgda_peers(
    const std::vector<int64_t>& remote_addrs,
    const std::vector<int32_t>& remote_keys,
    const std::vector<std::vector<int32_t>>& peer_qpns,
    const std::vector<std::vector<int32_t>>& peer_lids,
    const std::vector<int64_t>& subnet_prefixes,
    const std::vector<int64_t>& interface_ids,
    const std::vector<int>& active_ranks_mask) {
    native_buffer().sync_ibgda_peers(remote_addrs, remote_keys, peer_qpns,
                                     peer_lids, subnet_prefixes, interface_ids,
                                     active_ranks_mask);
}

std::tuple<int64_t, int32_t> MooncakeElasticBuffer::get_mr_info() {
    return native_buffer().get_mr_info();
}

std::tuple<int64_t, int64_t> MooncakeElasticBuffer::get_gid() {
    return native_buffer().get_gid();
}

std::vector<int32_t> MooncakeElasticBuffer::get_local_qpns() {
    return native_buffer().get_local_qpns();
}

std::vector<int32_t> MooncakeElasticBuffer::get_local_lids() {
    return native_buffer().get_local_lids();
}

std::vector<int32_t> MooncakeElasticBuffer::get_ipc_handle() {
    return native_buffer().get_ipc_handle();
}

void MooncakeElasticBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    native_buffer().sync_nvlink_ipc_handles(remote_handles, active_ranks_mask);
}

cudaStream_t MooncakeElasticBuffer::communication_stream() const {
    if (using_nccl()) {
#ifdef USE_NCCL_DEVICE
        return nccl_state_->comm_stream.stream();
#else
        throw std::logic_error(
            "NCCL ElasticBuffer state exists in a non-NCCL build");
#endif
    }
    if (!native_buffer_) {
        throw std::runtime_error("ElasticBuffer has already been destroyed");
    }
    return native_buffer_->comm_stream;
}

int MooncakeElasticBuffer::clock_rate_khz() const {
    if (using_nccl()) {
#ifdef USE_NCCL_DEVICE
        return nccl_state_->clock_rate_khz;
#else
        throw std::logic_error(
            "NCCL ElasticBuffer state exists in a non-NCCL build");
#endif
    }
    if (!native_buffer_) {
        throw std::runtime_error("ElasticBuffer has already been destroyed");
    }
    return native_buffer_->clock_rate_khz;
}
int64_t MooncakeElasticBuffer::calculate_buffer_size(
    int num_ranks, int64_t num_max_tokens_per_rank, int64_t hidden,
    int64_t num_topk, bool use_fp8_dispatch, bool allow_hybrid_mode,
    bool allow_multiple_reduction) {
    num_topk = std::max<int64_t>(1, num_topk);
    const int64_t dtype_bytes = use_fp8_dispatch ? 1 : 2;
    const int64_t scale_bytes =
        use_fp8_dispatch ? ceil_div_i64(hidden, 128) * 4 : 0;
    const int64_t token_bytes =
        align_i64(hidden * dtype_bytes, 32) + align_i64(scale_bytes, 32);
    const int64_t metadata_bytes = align_i64(
        num_topk * (sizeof(int) + sizeof(float)) + (1 + num_topk) * sizeof(int),
        32);
    const int64_t per_slot_bytes = token_bytes + metadata_bytes;
    const int64_t dispatch_bytes =
        num_ranks * num_max_tokens_per_rank * num_topk * per_slot_bytes * 2;
    const int64_t combine_factor = allow_multiple_reduction ? 3 : 4;
    const int64_t combine_bytes = dispatch_bytes * combine_factor;
    const int64_t hybrid_factor = allow_hybrid_mode && num_ranks > 1 ? 2 : 1;
    return elastic_workspace_num_bytes() + elastic_atomic_scratch_num_bytes() +
           hybrid_factor * (dispatch_bytes + combine_bytes);
}

std::tuple<int, int> MooncakeElasticBuffer::get_physical_domain_size() const {
    return {topology_.num_rdma_ranks, topology_.num_nvlink_ranks};
}

std::tuple<int, int> MooncakeElasticBuffer::get_logical_domain_size() const {
    return {topology_.num_scaleout_ranks, topology_.num_scaleup_ranks};
}

std::shared_ptr<void>
MooncakeElasticBuffer::ensure_deterministic_rank_count_buffer(int num_sms) {
    const int64_t required_bytes = static_cast<int64_t>(sizeof(int)) * num_sms *
                                   topology_.num_scaleup_ranks;
    if (deterministic_rank_count_buffer_ != nullptr &&
        deterministic_rank_count_buffer_bytes_ >= required_bytes) {
        return deterministic_rank_count_buffer_;
    }

    void* buffer_ptr = nullptr;
    CUDA_CHECK(cudaMalloc(&buffer_ptr, required_bytes));
    deterministic_rank_count_buffer_ =
        std::shared_ptr<void>(buffer_ptr, [](void* p) { cudaFree(p); });
    deterministic_rank_count_buffer_bytes_ = required_bytes;
    return deterministic_rank_count_buffer_;
}

int MooncakeElasticBuffer::get_theoretical_num_sms(int num_experts,
                                                   int num_topk) const {
    int device = 0;
    cudaGetDevice(&device);
    cudaDeviceProp prop{};
    cudaGetDeviceProperties(&prop, device);
    if (config_.prefer_overlap_with_compute) {
        return std::max(1, std::min(24, prop.multiProcessorCount / 4));
    }
    return std::max(1, std::min({40, prop.multiProcessorCount / 2,
                                 std::max(1, num_experts * num_topk)}));
}

std::optional<EventHandle> MooncakeElasticBuffer::dispatch(
    uint64_t x_ptr, int x_element_size, uint64_t sf_ptr, int num_tokens,
    int hidden, int num_sf_packs, int sf_token_stride, int sf_hidden_stride,
    uint64_t topk_idx_ptr, int num_topk, uint64_t topk_weights_ptr,
    uint64_t active_ranks_ptr, int num_experts, int num_max_tokens_per_rank,
    int expert_alignment, int num_sms, bool do_expand,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    bool cached_mode, int num_recv_tokens,
    uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t psum_num_recv_tokens_per_expert_ptr,
    uint64_t dst_buffer_slot_idx_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t recv_x_ptr,
    uint64_t recv_x_scales_ptr, uint64_t recv_topk_idx_ptr,
    uint64_t recv_topk_weights_ptr, uint64_t recv_src_metadata_ptr) {
    const bool use_sf = sf_ptr != 0;
    EP_HOST_ASSERT(num_experts % topology_.num_ranks == 0);

    const int num_local_experts = num_experts / topology_.num_ranks;
    // The copy epilogue uses `kNumMaxTokensPerRank * kNumRanks` as the
    // no-CPU-sync sentinel and then reads the real local receive count from the
    // GPU prefix-sum tensor. In hybrid mode each scale-up peer may receive
    // tokens forwarded from every scale-out rank, so the conservative output
    // capacity and sentinel must cover the full logical world, not just the
    // intra-node scale-up domain.
    const int max_num_recv_tokens =
        num_max_tokens_per_rank * topology_.num_ranks;
    EP_HOST_ASSERT(num_recv_tokens >= 0 &&
                   num_recv_tokens <= max_num_recv_tokens);
    EP_HOST_ASSERT(cached_mode || num_recv_tokens == max_num_recv_tokens);
    const int num_smem_bytes = device_smem_bytes_;
    const int num_channels_per_sm = 1;
    const int num_channels = num_sms * num_channels_per_sm;
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels_per_sm = using_nccl()
                                           ? kNcclElasticHybridChannelsPerSm
                                           : kIbgdaElasticHybridChannelsPerSm;
    const int hybrid_channels =
        use_hybrid ? hybrid_num_channels(num_sms, hybrid_channels_per_sm) : 0;
    const int hybrid_max_tokens_per_channel =
        use_hybrid ? hybrid_num_max_tokens_per_channel(
                         num_max_tokens_per_rank, num_sms,
                         hybrid_channels_per_sm)
                   : 0;

    EP_HOST_ASSERT(x_ptr != 0 && topk_idx_ptr != 0 && active_ranks_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_scaleup_rank_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_expert_ptr != 0);
    EP_HOST_ASSERT(dst_buffer_slot_idx_ptr != 0);
    EP_HOST_ASSERT(recv_x_ptr != 0 && recv_topk_idx_ptr != 0 &&
                   recv_src_metadata_ptr != 0);
    if (use_hybrid) {
        EP_HOST_ASSERT(token_metadata_at_forward_ptr != 0);
        EP_HOST_ASSERT(channel_linked_list_ptr != 0);
    }

    void* x = reinterpret_cast<void*>(x_ptr);
    void* sf = reinterpret_cast<void*>(sf_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* topk_weights = reinterpret_cast<float*>(topk_weights_ptr);
    auto* active_ranks = reinterpret_cast<int*>(active_ranks_ptr);
    auto* psum_num_recv_tokens_per_scaleup_rank =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    auto* psum_num_recv_tokens_per_expert =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_expert_ptr);
    auto* dst_buffer_slot_idx = reinterpret_cast<int*>(dst_buffer_slot_idx_ptr);
    auto* token_metadata_at_forward =
        reinterpret_cast<int*>(token_metadata_at_forward_ptr);
    auto* channel_linked_list = reinterpret_cast<int*>(channel_linked_list_ptr);
    void* recv_x = reinterpret_cast<void*>(recv_x_ptr);
    void* recv_x_scales = reinterpret_cast<void*>(recv_x_scales_ptr);
    auto* recv_topk_idx = reinterpret_cast<int64_t*>(recv_topk_idx_ptr);
    auto* recv_topk_weights = reinterpret_cast<float*>(recv_topk_weights_ptr);
    auto* recv_src_metadata = reinterpret_cast<int*>(recv_src_metadata_ptr);

    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = communication_stream();
    stream_wait(launch_stream, compute_stream_raw);

    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(clock_rate_khz()) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(timeout_cycles);

    std::shared_ptr<void> deterministic_rank_count_buffer;
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA non-hybrid dispatch always runs
    // launch_musa_elastic_prepare_dispatch(), which assigns slots and publishes
    // counts without cooperative grid sync.
    const bool run_deterministic_prologue = false;
#else
    const bool run_deterministic_prologue =
        config_.deterministic && !cached_mode && !use_hybrid;
#endif
    if (run_deterministic_prologue) {
        deterministic_rank_count_buffer =
            ensure_deterministic_rank_count_buffer(num_sms);
        launch_elastic_dispatch_deterministic_prologue(
            topk_idx, static_cast<int*>(deterministic_rank_count_buffer.get()),
            dst_buffer_slot_idx, num_tokens, num_max_tokens_per_rank,
            num_experts, num_topk, topology_.scaleup_rank_idx,
            topology_.num_scaleup_ranks, num_sms, num_smem_bytes,
            launch_stream);
    }

    launch_mooncake_elastic_dispatch(
        x, sf, topk_idx, topk_weights, nullptr, nullptr,
        psum_num_recv_tokens_per_scaleup_rank, psum_num_recv_tokens_per_expert,
        dst_buffer_slot_idx, token_metadata_at_forward, num_tokens,
        num_max_tokens_per_rank, hidden, x_element_size, num_sf_packs,
        sf_token_stride, sf_hidden_stride, num_experts, num_topk,
        expert_alignment, num_sms,
        use_hybrid ? hybrid_channels_per_sm : num_channels_per_sm,
        num_smem_bytes, cached_mode, config_.deterministic, false, launch_ctx,
        launch_stream);

    const int recv_sf_token_stride = num_sf_packs;
    const int recv_sf_hidden_stride = 1;
    auto* epilogue_psum_num_recv_tokens_per_expert =
        do_expand ? psum_num_recv_tokens_per_expert
                  : psum_num_recv_tokens_per_expert + 1;

    launch_mooncake_elastic_dispatch_copy_epilogue(
        recv_x, recv_x_scales, recv_topk_idx, recv_topk_weights,
        recv_src_metadata, channel_linked_list, num_recv_tokens,
        num_max_tokens_per_rank, hidden, x_element_size, num_sf_packs,
        recv_sf_token_stride, recv_sf_hidden_stride, num_experts, num_topk,
        num_sms, physical_num_sms_, num_smem_bytes,
        use_hybrid ? hybrid_channels : num_channels,
        do_expand, cached_mode, launch_ctx,
        psum_num_recv_tokens_per_scaleup_rank,
        epilogue_psum_num_recv_tokens_per_expert, launch_stream);

    (void)active_ranks;
    (void)num_local_experts;
    (void)hybrid_max_tokens_per_channel;
    if (!async_with_compute_stream) {
        stream_wait(compute_stream_raw, launch_stream);
        return std::nullopt;
    }
    return EventHandle(reinterpret_cast<uint64_t>(launch_stream),
                       deterministic_rank_count_buffer);
}

std::optional<EventHandle> MooncakeElasticBuffer::combine(
    uint64_t x_ptr, int num_input_tokens, int hidden, uint64_t topk_idx_ptr,
    int num_combined_tokens, int num_topk, uint64_t topk_weights_ptr,
    uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t recv_src_metadata_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t active_ranks_ptr,
    int num_experts, int num_max_tokens_per_rank, bool do_expand, int num_sms,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    uint64_t combined_x_ptr) {
    EP_HOST_ASSERT(x_ptr != 0 && topk_idx_ptr != 0 && topk_weights_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_scaleup_rank_ptr != 0);
    EP_HOST_ASSERT(recv_src_metadata_ptr != 0 && active_ranks_ptr != 0);
    EP_HOST_ASSERT(combined_x_ptr != 0);
    void* x = reinterpret_cast<void*>(x_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* topk_weights = reinterpret_cast<float*>(topk_weights_ptr);
    auto* psum_num_recv_tokens_per_scaleup_rank =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    auto* recv_src_metadata = reinterpret_cast<int*>(recv_src_metadata_ptr);
    auto* token_metadata_at_forward =
        reinterpret_cast<int*>(token_metadata_at_forward_ptr);
    auto* channel_linked_list = reinterpret_cast<int*>(channel_linked_list_ptr);
    auto* active_ranks = reinterpret_cast<int*>(active_ranks_ptr);
    void* combined_x = reinterpret_cast<void*>(combined_x_ptr);

    const int num_smem_bytes = device_smem_bytes_;
    const int num_channels = std::max(1, num_sms);
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels_per_sm = using_nccl()
                                           ? kNcclElasticHybridChannelsPerSm
                                           : kIbgdaElasticHybridChannelsPerSm;
    const int hybrid_channels =
        use_hybrid ? hybrid_num_channels(num_sms, hybrid_channels_per_sm) : 0;
    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = communication_stream();
    stream_wait(launch_stream, compute_stream_raw);
    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(clock_rate_khz()) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(timeout_cycles);
    void* reduce_buffer = launch_mooncake_elastic_combine(
        x, topk_weights, recv_src_metadata,
        psum_num_recv_tokens_per_scaleup_rank, token_metadata_at_forward,
        channel_linked_list, num_input_tokens, num_max_tokens_per_rank, hidden,
        num_experts, num_topk, num_sms, num_smem_bytes,
        use_hybrid ? hybrid_channels : num_channels, do_expand,
        config_.allow_multiple_reduction, launch_ctx, launch_stream);

    launch_mooncake_elastic_combine_reduce_epilogue(
        combined_x, topk_weights, topk_idx, num_combined_tokens,
        num_max_tokens_per_rank, hidden, num_experts, num_topk, reduce_buffer,
        nullptr, nullptr, num_sms, physical_num_sms_, num_smem_bytes,
        do_expand,
        config_.allow_multiple_reduction, launch_ctx, launch_stream);

    (void)active_ranks;
    if (!async_with_compute_stream) {
        stream_wait(compute_stream_raw, launch_stream);
        return std::nullopt;
    }
    return EventHandle(reinterpret_cast<uint64_t>(launch_stream));
}

ElasticTopology MooncakeElasticBuffer::discover_topology(
    int rank, int num_ranks, bool allow_hybrid_mode) {
    int device_count = 1;
    cudaGetDeviceCount(&device_count);
    int num_local_ranks =
        getenv_int("MOONCAKE_EP_NUM_LOCAL_RANKS",
                   std::max(1, std::min(num_ranks, device_count)));
    num_local_ranks = std::max(1, std::min(num_local_ranks, num_ranks));

    ElasticTopology topology;
    topology.rank_idx = rank;
    topology.num_ranks = num_ranks;
    topology.num_rdma_ranks =
        static_cast<int>(ceil_div_i64(num_ranks, num_local_ranks));
    topology.num_nvlink_ranks = num_local_ranks;
    if (allow_hybrid_mode && topology.num_rdma_ranks > 1) {
        topology.num_scaleout_ranks = topology.num_rdma_ranks;
        topology.num_scaleup_ranks = topology.num_nvlink_ranks;
        topology.hybrid_enabled = true;
    } else {
        topology.num_scaleout_ranks = 1;
        topology.num_scaleup_ranks = num_ranks;
        topology.hybrid_enabled = false;
    }
    topology.scaleout_rank_idx = rank / topology.num_scaleup_ranks;
    topology.scaleup_rank_idx = rank % topology.num_scaleup_ranks;
    topology.scaleup_lsa =
        topology.hybrid_enabled || topology.num_rdma_ranks == 1;
    return topology;
}

}  // namespace mooncake
