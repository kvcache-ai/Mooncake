#include <mooncake_ep_buffer.h>
#include <arpa/inet.h>
#include <glog/logging.h>

#ifdef MOONCAKE_EP_USE_MUSA
#include <tent/device/mtlink.h>
#else
#include <tent/device/ibgda.h>
#include <tent/device/nvlink.h>
#endif

#ifdef MOONCAKE_EP_USE_MUSA
// MUSA: no InfiniBand headers available
#else
#include <infiniband/mlx5dv.h>
#endif

namespace mooncake {

// Check if all GPUs support fabric memory handles (MNNVL).
// Mirrors the check in nvlink_transport.cpp.
static bool supportFabricMem() {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no fabric memory support yet
    return false;
#else
    return tent::nvLinkSupportsFabricMemory();
#endif
}


MooncakeEpBuffer::MooncakeEpBuffer(int rank, int num_ranks,
                                   int64_t num_ep_buffer_bytes,
                                   std::string device_name)
    : rank(rank),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(num_ep_buffer_bytes),
      device_name(std::move(device_name)),
#ifdef MOONCAKE_EP_USE_MUSA
      comm_stream(at::musa::getStreamFromPool(true)) {
#else
      comm_stream(at::cuda::getStreamFromPool(true)) {
#endif
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;
    // Get ranks
    CUDA_CHECK(cudaGetDevice(&device_id));
    CUDA_CHECK(cudaDeviceGetAttribute(&clock_rate_khz, cudaDevAttrClockRate,
                                      device_id));

    // Allocate gdr_buffer. On MNNVL clusters, use cuMemCreate with a fabric
    // handle so the buffer is accessible cross-node via NVLink fabric.
    // On IB clusters or single-node setups, fall back to cudaMalloc.
    use_fabric_mem_ = supportFabricMem();
#ifndef MOONCAKE_EP_USE_MUSA
    if (use_fabric_mem_) {
        CUdevice cu_dev;
        CUresult res = cuDeviceGet(&cu_dev, device_id);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "[EP] cuDeviceGet failed: " << res;
            throw std::runtime_error("cuDeviceGet failed");
        }

        CUmemAllocationProp prop = {};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = cu_dev;
        prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;

        int rdma_flag = 0;
        cuDeviceGetAttribute(
            &rdma_flag,
            CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
            cu_dev);
        if (rdma_flag) prop.allocFlags.gpuDirectRDMACapable = 1;

        size_t granularity = 0;
        res = cuMemGetAllocationGranularity(&granularity, &prop,
                                            CU_MEM_ALLOC_GRANULARITY_MINIMUM);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "[EP] cuMemGetAllocationGranularity failed: " << res;
            throw std::runtime_error("cuMemGetAllocationGranularity failed");
        }

        fabric_alloc_size_ =
            (num_ep_buffer_bytes + granularity - 1) & ~(granularity - 1);
        if (fabric_alloc_size_ == 0) fabric_alloc_size_ = granularity;

        res = cuMemCreate(&fabric_mem_handle_, fabric_alloc_size_, &prop, 0);
        if (res != CUDA_SUCCESS) {
            LOG(ERROR) << "[EP] cuMemCreate(FABRIC) failed: " << res;
            throw std::runtime_error("cuMemCreate failed");
        }

        CUdeviceptr dptr = 0;
        res = cuMemAddressReserve(&dptr, fabric_alloc_size_, granularity, 0, 0);
        if (res != CUDA_SUCCESS) {
            cuMemRelease(fabric_mem_handle_);
            LOG(ERROR) << "[EP] cuMemAddressReserve failed: " << res;
            throw std::runtime_error("cuMemAddressReserve failed");
        }

        res = cuMemMap(dptr, fabric_alloc_size_, 0, fabric_mem_handle_, 0);
        if (res != CUDA_SUCCESS) {
            cuMemAddressFree(dptr, fabric_alloc_size_);
            cuMemRelease(fabric_mem_handle_);
            LOG(ERROR) << "[EP] cuMemMap failed: " << res;
            throw std::runtime_error("cuMemMap failed");
        }

        // Grant read/write access to all devices in the fabric clique
        int device_count = 0;
        cudaGetDeviceCount(&device_count);
        std::vector<CUmemAccessDesc> access(device_count);
        for (int i = 0; i < device_count; ++i) {
            access[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            access[i].location.id = i;
            access[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        }
        res = cuMemSetAccess(dptr, fabric_alloc_size_, access.data(),
                             device_count);
        if (res != CUDA_SUCCESS) {
            cuMemUnmap(dptr, fabric_alloc_size_);
            cuMemAddressFree(dptr, fabric_alloc_size_);
            cuMemRelease(fabric_mem_handle_);
            LOG(ERROR) << "[EP] cuMemSetAccess failed: " << res;
            throw std::runtime_error("cuMemSetAccess failed");
        }

        gdr_buffer = reinterpret_cast<void*>(dptr);
        LOG(INFO) << "[EP] Allocated " << fabric_alloc_size_
                  << " bytes with fabric handle on GPU " << device_id;
    } else {
        CUDA_CHECK(cudaMalloc(&gdr_buffer, num_ep_buffer_bytes));
    }
#else  // MOONCAKE_EP_USE_MUSA
    // MUSA: no fabric memory, simple musaMalloc
    CUDA_CHECK(musaMalloc(&gdr_buffer, num_ep_buffer_bytes));
#endif  // MOONCAKE_EP_USE_MUSA
    CUDA_CHECK(cudaMalloc(&raddrs, num_ranks * sizeof(uint64_t)));
    CUDA_CHECK(cudaMalloc(&rkeys, num_ranks * sizeof(uint32_t)));
#ifndef MOONCAKE_EP_USE_MUSA
    CUDA_CHECK(cudaMalloc(
        &qp_devctxs, USE_QP_COUNT * tent::ibGdaQueuePairDeviceContextSize()));
#endif

    // Create the P2P device transport (auto-detects NVLink/MTLink).
    transport_ = tent::createP2pDeviceTransport();
    auto p2p_status = transport_->allocatePeerAccessTables(rank, num_ranks);
    if (!p2p_status.ok()) {
        LOG(ERROR) << "[EP] Failed to allocate P2P peer tables: "
                   << std::string(p2p_status.message());
        throw std::runtime_error("Failed to allocate P2P peer tables");
    }

#ifndef MOONCAKE_EP_USE_MUSA
    int ret = init_ibgda();
    if (ret != 0) {
        ibgda_disabled_ = true;
    }
#else
    // MUSA: no IBGDA, always disabled
    ibgda_disabled_ = true;
#endif

    // Create 32 MiB workspace
    CUDA_CHECK(cudaMalloc(&workspace, NUM_WORKSPACE_BYTES));
    CUDA_CHECK(cudaMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));
}

MooncakeEpBuffer::~MooncakeEpBuffer() noexcept(false) {
    // Destroy transports first — IBGDA may need to deregister memory while
    // gdr_buffer is still valid.
#ifndef MOONCAKE_EP_USE_MUSA
    if (ibgda_transport_) ibgda_transport_.reset();
#endif
    transport_.reset();

#ifndef MOONCAKE_EP_USE_MUSA
    if (use_fabric_mem_) {
        CUdeviceptr dptr = reinterpret_cast<CUdeviceptr>(gdr_buffer);
        cuMemUnmap(dptr, fabric_alloc_size_);
        cuMemAddressFree(dptr, fabric_alloc_size_);
        cuMemRelease(fabric_mem_handle_);
    } else {
        cudaFree(gdr_buffer);
    }
#else
    musaFree(gdr_buffer);
#endif
    cudaFree(raddrs);
    cudaFree(rkeys);
#ifndef MOONCAKE_EP_USE_MUSA
    cudaFree(qp_devctxs);
#endif
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
           torch::Tensor, torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::dispatch(const torch::Tensor& x,
                           const torch::Tensor& topk_idx,
                           torch::Tensor& active_ranks,
                           int num_max_dispatch_tokens_per_rank,
                           int num_experts, int timeout_us, bool use_fp8,
                           bool async, bool return_recv_hook) {
    // Tensor checks
    // By default using `ptp128c` FP8 cast
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(1) % sizeof(int4) == 0 and x.size(1) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(x.size(0) == topk_idx.size(0) and
                   x.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(num_experts % num_ranks == 0);
    EP_HOST_ASSERT(USE_QP_COUNT % num_ranks == 0);

    auto num_tokens = static_cast<int>(x.size(0)),
         hidden = static_cast<int>(x.size(1));
    auto num_scales = hidden / 128,
         num_topk = static_cast<int>(topk_idx.size(1));
    int num_local_experts = num_experts / num_ranks;

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
#ifdef MOONCAKE_EP_USE_MUSA
    auto compute_stream = at::musa::getCurrentMUSAStream();
#else
    auto compute_stream = at::cuda::getCurrentCUDAStream();
#endif
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

    // Allocate packed tensors
    auto packed_recv_x = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn : torch::kBFloat16));
    auto packed_recv_src_info = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank},
        torch::dtype(torch::kInt32).device(kDeviceType));
    auto packed_recv_layout_range =
        torch::empty({num_local_experts, num_ranks},
                     torch::dtype(torch::kInt64).device(kDeviceType));
    auto packed_recv_count = torch::zeros(
        {num_local_experts}, torch::dtype(torch::kInt32).device(kDeviceType));

    // Allocate column-majored scales
    auto packed_recv_x_scales = std::optional<torch::Tensor>();
    float* packed_recv_x_scales_ptr = nullptr;
    if (use_fp8) {
        EP_HOST_ASSERT((num_ranks * num_max_dispatch_tokens_per_rank) % 4 ==
                           0 and
                       "TMA requires the number of tokens to be multiple of 4");
        packed_recv_x_scales =
            torch::empty({num_local_experts, num_scales,
                          num_ranks * num_max_dispatch_tokens_per_rank},
                         torch::dtype(torch::kFloat32).device(kDeviceType));
        packed_recv_x_scales =
            torch::transpose(packed_recv_x_scales.value(), 1, 2);
        packed_recv_x_scales_ptr = packed_recv_x_scales->data_ptr<float>();
    }

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    // Get P2P table pointers from the unified transport
    auto* nvlink_available = transport_->availableTablePtr();
    auto* ipc_peer_ptrs = transport_->peerPtrsTablePtr();

    auto launcher = [=](int phases) {
        mooncake::dispatch(
            packed_recv_x.data_ptr(), packed_recv_x_scales_ptr,
            packed_recv_src_info.data_ptr<int>(),
            packed_recv_layout_range.data_ptr<int64_t>(),
            packed_recv_count.data_ptr<int>(), active_ranks.data_ptr<int32_t>(),
            gdr_buffer, buffer.rdma_send_signal_buffer,
            buffer.rdma_recv_signal_buffer, buffer.rdma_send_data_buffer,
            buffer.rdma_recv_data_buffer, nullptr, nullptr, raddrs, rkeys,
            qp_devctxs, nvlink_available, ipc_peer_ptrs, x.data_ptr(),
            topk_idx.data_ptr<int64_t>(), next_buffer.rdma_recv_signal_buffer,
            num_tokens, hidden, num_max_dispatch_tokens_per_rank, num_topk,
            num_experts, rank, num_ranks, use_fp8, workspace,
#ifdef MOONCAKE_EP_USE_MUSA
            launch_stream.stream(),
#else
            launch_stream,
#endif
            timeout_ticks, phases);
    };
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no cooperative grid sync, must use separate kernel launches
    launcher(LOW_LATENCY_SEND_PHASE);
#else
    launcher(return_recv_hook
                 ? LOW_LATENCY_SEND_PHASE
                 : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));
#endif

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: always launch recv phase as a separate kernel
    recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
#else
    if (return_recv_hook)
        recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
#endif

    // Return values
    return {packed_recv_x,
            packed_recv_x_scales,
            packed_recv_count,
            packed_recv_src_info,
            packed_recv_layout_range,
            event,
            recv_hook};
}

std::tuple<torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::combine(const torch::Tensor& x, const torch::Tensor& topk_idx,
                          const torch::Tensor& topk_weights,
                          const torch::Tensor& src_info,
                          const torch::Tensor& layout_range,
                          torch::Tensor& active_ranks,
                          int num_max_dispatch_tokens_per_rank, int num_experts,
                          int timeout_us, bool zero_copy, bool async,
                          bool return_recv_hook,
                          const std::optional<torch::Tensor>& out) {
    // Tensor checks
    EP_HOST_ASSERT(x.dim() == 3 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(0) == num_experts / num_ranks);
    EP_HOST_ASSERT(x.size(1) == num_ranks * num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(x.size(2) % sizeof(int4) == 0 and x.size(2) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.size(0) == topk_weights.size(0) and
                   topk_idx.size(1) == topk_weights.size(1));
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(topk_weights.dim() == 2 and topk_weights.is_contiguous());
    EP_HOST_ASSERT(topk_weights.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_weights.scalar_type() == torch::kFloat32);
    EP_HOST_ASSERT(src_info.dim() == 2 and src_info.is_contiguous());
    EP_HOST_ASSERT(src_info.scalar_type() == torch::kInt32 and
                   x.size(0) == src_info.size(0));
    EP_HOST_ASSERT(layout_range.dim() == 2 and layout_range.is_contiguous());
    EP_HOST_ASSERT(layout_range.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(layout_range.size(0) == num_experts / num_ranks and
                   layout_range.size(1) == num_ranks);
    auto hidden = static_cast<int>(x.size(2));
    auto num_local_experts = num_experts / num_ranks,
         num_topk = static_cast<int>(topk_weights.size(1));
    auto num_combined_tokens = static_cast<int>(topk_weights.size(0));

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
#ifdef MOONCAKE_EP_USE_MUSA
    auto compute_stream = at::musa::getCurrentMUSAStream();
#else
    auto compute_stream = at::cuda::getCurrentCUDAStream();
#endif
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

    // Allocate output tensor
    torch::Tensor combined_x;
    if (out.has_value()) {
        EP_HOST_ASSERT(out->dim() == 2 and out->is_contiguous());
        EP_HOST_ASSERT(out->size(0) == num_combined_tokens and
                       out->size(1) == hidden);
        EP_HOST_ASSERT(out->scalar_type() == x.scalar_type());
        combined_x = out.value();
    } else {
        combined_x = torch::empty({num_combined_tokens, hidden}, x.options());
    }

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    // Get P2P table pointers from the unified transport
    auto* nvlink_available = transport_->availableTablePtr();
    auto* ipc_peer_ptrs = transport_->peerPtrsTablePtr();

    // Kernel launch
    auto launcher = [=](int phases) {
        mooncake::combine(
            combined_x.data_ptr(), active_ranks.data_ptr<int32_t>(), gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer, nullptr,
            nullptr, raddrs, rkeys, qp_devctxs, nvlink_available, ipc_peer_ptrs,
            x.data_ptr(), topk_idx.data_ptr<int64_t>(),
            topk_weights.data_ptr<float>(), src_info.data_ptr<int>(),
            layout_range.data_ptr<int64_t>(),
            next_buffer.rdma_recv_signal_buffer, num_combined_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, workspace,
#ifdef MOONCAKE_EP_USE_MUSA
            launch_stream.stream(),
#else
            launch_stream,
#endif
            timeout_ticks, phases,
            zero_copy);
    };
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no cooperative grid sync, must use separate kernel launches
    launcher(LOW_LATENCY_SEND_PHASE);
#else
    launcher(return_recv_hook
                 ? LOW_LATENCY_SEND_PHASE
                 : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));
#endif

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: always launch recv phase as a separate kernel
    recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
#else
    if (return_recv_hook)
        recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
#endif

    // Return values
    return {combined_x, event, recv_hook};
}

torch::Tensor MooncakeEpBuffer::get_next_combine_buffer(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) {
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);

    auto buffer = layout.buffers[buffer_idx];
    auto dtype = torch::kBFloat16;
    size_t num_bytes_per_combine_msg = hidden * sizeof(nv_bfloat16);
    auto num_msg_elems = static_cast<int>(num_bytes_per_combine_msg /
                                          elementSize(torch::kBFloat16));

    EP_HOST_ASSERT(num_bytes_per_combine_msg % elementSize(torch::kBFloat16) ==
                   0);
    return torch::from_blob(
        buffer.rdma_send_data_buffer,
        {num_experts / num_ranks, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        {num_ranks * num_max_dispatch_tokens_per_rank * num_msg_elems,
         num_msg_elems, 1},
        torch::TensorOptions().dtype(dtype).device(kDeviceType));
}

int MooncakeEpBuffer::init_ibgda() {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no IBGDA support
    return -1;
#else
    // Create a separate IBGDA transport for RDMA operations.
    // The NVLink transport handles P2P; IBGDA handles RDMA.
    auto ibgda = tent::createIbGdaDeviceTransport();
    auto status = ibgda->initializeRdmaDevice(device_name, 1);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA device initialization failed: "
                   << status.ToString();
        return -1;
    }
    uint32_t lkey = 0;
    uint32_t rkey = 0;
    status = ibgda->registerMemory(gdr_buffer, num_ep_buffer_bytes, lkey, rkey);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA memory registration failed: "
                   << status.ToString();
        return -1;
    }
    gid_index_ = ibgda->gidIndex();
    LOG(INFO) << "[EP] GPU " << device_id << " TENT IBGDA initialized with GID index " << gid_index_;

    // Allocate ctrl_buf without zero-initialization. Individual regions will be
    // initialized as needed: CQ needs -1 (hardware requirement), DBR needs 0.
    // WQ doesn't need initialization as it's zeroed before each use.
    LOG(INFO) << "[EP] GPU " << device_id << " allocating control buffer ("
              << CTRL_BUF_SIZE << " bytes)...";
    status = ibgda->allocateControlBuffer(CTRL_BUF_SIZE);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA control buffer allocation failed: "
                   << status.ToString();
        fprintf(stderr,
                "If the error is `Bad address`, probably because your GPU "
                "does not support GPUDirect RDMA.\n");
        (void)ibgda->unregisterMemory(gdr_buffer);
        return -1;
    }
    ctrl_buf = ibgda->controlBuffer();
    LOG(INFO) << "[EP] GPU " << device_id << " control buffer allocated, creating QPs...";

    status = ibgda->createQueuePairs(
        USE_QP_COUNT, 16384, reinterpret_cast<void*>(comm_stream.stream()),
        qp_devctxs);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA QP creation failed: "
                   << status.ToString();
        return -1;
    }
    LOG(INFO) << "[EP] GPU " << device_id << " QPs created successfully";
    is_roce_ = ibgda->isRoce();

    // Store the IBGDA transport.  On CUDA, we need both NVLink (for P2P)
    // and IBGDA (for RDMA).  transport_ = NVLink; ibgda_transport_ = IBGDA.
    ibgda_transport_ = std::move(ibgda);
    return 0;
#endif  // MOONCAKE_EP_USE_MUSA
}

bool MooncakeEpBuffer::update_local_qpns() {
#ifdef MOONCAKE_EP_USE_MUSA
    return true;
#else
    auto status = ibgda_transport_->recreateQueuePairs(
        USE_QP_COUNT, 16384, reinterpret_cast<void*>(comm_stream.stream()),
        qp_devctxs);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA QP recreation failed: "
                   << status.ToString();
        ibgda_disabled_ = true;
        return false;
    }
    is_roce_ = ibgda_transport_->isRoce();
    return true;
#endif  // MOONCAKE_EP_USE_MUSA
}

void MooncakeEpBuffer::sync_ib(const std::vector<int64_t>&,
                               const std::vector<int32_t>&,
                               const std::vector<int32_t>&,
                               const std::vector<int32_t>&,
                               const std::vector<int>&) {
    // Legacy path — not used in TENT mode.
}

void MooncakeEpBuffer::sync_roce(const std::vector<int64_t>&,
                                 const std::vector<int32_t>&,
                                 const std::vector<int32_t>&,
                                 const std::vector<int64_t>&,
                                 const std::vector<int64_t>&,
                                 const std::vector<int>&) {
    // Legacy path — not used in TENT mode.
}

void MooncakeEpBuffer::sync_ibgda_peers(
    const std::vector<int64_t>& remote_addrs,
    const std::vector<int32_t>& remote_keys,
    const std::vector<std::vector<int32_t>>& peer_qpns,
    const std::vector<std::vector<int32_t>>& peer_lids,
    const std::vector<int64_t>& subnet_prefixes,
    const std::vector<int64_t>& interface_ids,
    const std::vector<int>& active_ranks_mask) {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no IBGDA peer sync
#else
    tent::RdmaPeerConnectInfo info;
    info.remote_addrs = remote_addrs;
    info.remote_keys = remote_keys;
    info.peer_qpns = peer_qpns;
    info.peer_lids = peer_lids;
    info.subnet_prefixes = subnet_prefixes;
    info.interface_ids = interface_ids;
    info.active_ranks_mask = active_ranks_mask;
    info.rank = rank;
    info.num_ranks = num_ranks;
    info.raddrs = raddrs;
    info.rkeys = rkeys;
    auto status = ibgda_transport_->connectRdmaPeers(info);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA peer sync failed: "
                   << status.ToString();
        exit(1);
    }
#endif  // MOONCAKE_EP_USE_MUSA
}

std::vector<int32_t> MooncakeEpBuffer::get_ipc_handle() {
    if (use_fabric_mem_) {
        // Fabric memory is globally accessible via cuMemSetAccess — no IPC
        // handle exchange needed. Return an empty vector so the caller knows
        // to skip IPC for this rank.
        return {};
    }
    std::vector<int32_t> handle_words;
    auto status = transport_->exportIpcHandle(device_id, gdr_buffer, handle_words);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to export IPC handle: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to export IPC handle");
    }
    return handle_words;
}

void MooncakeEpBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    auto status = transport_->configurePeers(
        device_id, gdr_buffer, remote_handles, active_ranks_mask);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to configure P2P peers: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to configure P2P peers");
    }
}

}  // namespace mooncake
