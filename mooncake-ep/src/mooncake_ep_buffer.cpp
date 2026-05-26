#include <mooncake_ep_buffer.h>
#include <arpa/inet.h>
#include <glog/logging.h>

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
#ifdef MOONCAKE_EP_USE_TENT
    return tent::nvLinkSupportsFabricMemory();
#else
    const char* nvlink_ipc = getenv("MC_USE_NVLINK_IPC");

    bool fabric_enabled = nvlink_ipc && strcmp(nvlink_ipc, "0") == 0;
    if (!fabric_enabled) return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) return false;

    for (int dev = 0; dev < num_devices; ++dev) {
        int supported = 0;
        cuDeviceGetAttribute(
            &supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
        if (!supported) return false;
    }
    return true;
#endif
#endif  // MOONCAKE_EP_USE_MUSA
}

#ifndef MOONCAKE_EP_USE_TENT
// Check if IPv6 address is an IPv4-mapped address (::ffff:x.x.x.x)
static inline bool ipv6_addr_v4mapped(const struct in6_addr* a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) == 0 &&
            a->s6_addr32[2] == htonl(0x0000ffff));
}

// Dynamically find the best GID index (RoCE v2 + IPv4-mapped address, or IB)
// Returns GID index on success, -1 on failure
static int findBestGidIndex(ibv_context* ctx, uint8_t port,
                            ibv_port_attr& port_attr) {
    for (int i = 0; i < port_attr.gid_tbl_len; i++) {
        ibv_gid_entry gid_entry;
        int ret = ibv_query_gid_ex(ctx, port, i, &gid_entry, 0);
        if (ret) {
            continue;
        }

        bool is_v4mapped = ipv6_addr_v4mapped(
            reinterpret_cast<const struct in6_addr*>(gid_entry.gid.raw));

        // Look for IPv4-mapped address + RoCE v2, or IB type
        if ((is_v4mapped && gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            gid_entry.gid_type == IBV_GID_TYPE_IB) {
            return i;
        }
    }
    return -1;
}
#endif

void MooncakeEpBuffer::refresh_tent_ibgda_context() {
#ifndef MOONCAKE_EP_USE_MUSA
    tent_ibgda_ctx_.abi_version = tent::kIbGdaDeviceContextAbiVersion;
    tent_ibgda_ctx_.rank = rank;
    tent_ibgda_ctx_.num_ranks = num_ranks;
    tent_ibgda_ctx_.num_qps = USE_QP_COUNT;
    tent_ibgda_ctx_.raddrs = raddrs;
    tent_ibgda_ctx_.rkeys = rkeys;
    tent_ibgda_ctx_.qp_devctxs = qp_devctxs;
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
#ifdef MOONCAKE_EP_USE_TENT
    CUDA_CHECK(cudaMalloc(
        &qp_devctxs, USE_QP_COUNT * tent::ibGdaQueuePairDeviceContextSize()));
#else
    CUDA_CHECK(cudaMalloc(&qp_devctxs,
                          USE_QP_COUNT * sizeof(mlx5gda_qp_devctx)));
#endif
    refresh_tent_ibgda_context();
#endif  // MOONCAKE_EP_USE_MUSA

    // Allocate NVLink/MTLink P2P arrays
#if defined(MOONCAKE_EP_USE_TENT) && defined(MOONCAKE_EP_USE_MUSA)
    tent_mtlink_transport_ = tent::createMtLinkDeviceTransport();
    auto mtlink_status =
        tent_mtlink_transport_->allocatePeerAccessTables(rank, num_ranks);
    if (!mtlink_status.ok()) {
        LOG(ERROR) << "[EP] Failed to allocate TENT MTLink peer tables: "
                   << std::string(mtlink_status.message());
        throw std::runtime_error("Failed to allocate TENT MTLink peer tables");
    }
    tent_mtlink_ctx_ = tent_mtlink_transport_->deviceContext();
    nvlink_available = tent_mtlink_ctx_.available;
    ipc_peer_ptrs = tent_mtlink_ctx_.peer_ptrs;
#elif defined(MOONCAKE_EP_USE_TENT)
    tent_nvlink_transport_ = tent::createNvLinkDeviceTransport();
    auto nvlink_status =
        tent_nvlink_transport_->allocatePeerAccessTables(rank, num_ranks);
    if (!nvlink_status.ok()) {
        LOG(ERROR) << "[EP] Failed to allocate TENT NVLink peer tables: "
                   << std::string(nvlink_status.message());
        throw std::runtime_error("Failed to allocate TENT NVLink peer tables");
    }
    tent_nvlink_ctx_ = tent_nvlink_transport_->deviceContext();
    nvlink_available = tent_nvlink_ctx_.available;
    ipc_peer_ptrs = tent_nvlink_ctx_.peer_ptrs;
#else
    CUDA_CHECK(cudaMalloc(&nvlink_available, num_ranks * sizeof(int32_t)));
    CUDA_CHECK(cudaMemset(nvlink_available, 0, num_ranks * sizeof(int32_t)));
    CUDA_CHECK(cudaMallocHost(&ipc_peer_ptrs_host, num_ranks * sizeof(void*)));
    CUDA_CHECK(cudaMalloc(&ipc_peer_ptrs, num_ranks * sizeof(void*)));
    for (int i = 0; i < num_ranks; ++i) {
        ipc_peer_ptrs_host[i] = nullptr;
    }
    CUDA_CHECK(cudaMemset(ipc_peer_ptrs, 0, num_ranks * sizeof(void*)));
#endif

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
#if !defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
    for (auto* qp : qps) {
        if (qp && ctrl_buf_heap) {
            mlx5gda_destroy_qp(ctrl_buf_heap, qp);
        }
    }
    qps.clear();
    if (ctrl_buf_heap) memheap_destroy(ctrl_buf_heap);
#endif
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
    // TENT owns QP/heap/control-buffer/PD/MR in the migration path. It must be
    // destroyed before gdr_buffer free so its MR is deregistered while the GPU
    // buffer is still valid.
    if (tent_ibgda_transport_) tent_ibgda_transport_.reset();
#elif !defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
    if (ctrl_buf_umem) mlx5dv_devx_umem_dereg(ctrl_buf_umem);
    if (mr) ibv_dereg_mr(mr);
    if (pd) ibv_dealloc_pd(pd);
    if (ctrl_buf) cudaFree(ctrl_buf);
#endif
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
#if !defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
    if (nvlink_available) cudaFree(nvlink_available);
    if (ipc_peer_ptrs) cudaFree(ipc_peer_ptrs);
    if (ipc_peer_ptrs_host) {
        // Close IPC handles
        for (int i = 0; i < num_ranks; ++i) {
            if (ipc_peer_ptrs_host[i] != nullptr &&
                ipc_peer_ptrs_host[i] != gdr_buffer) {
                cudaIpcCloseMemHandle(ipc_peer_ptrs_host[i]);
            }
        }
        cudaFreeHost(ipc_peer_ptrs_host);
    }
#elif defined(MOONCAKE_EP_USE_TENT) && defined(MOONCAKE_EP_USE_MUSA)
    if (tent_mtlink_transport_) tent_mtlink_transport_.reset();
#elif defined(MOONCAKE_EP_USE_TENT)
    if (tent_nvlink_transport_) tent_nvlink_transport_.reset();
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
            num_experts, rank, num_ranks, use_fp8, workspace, launch_stream,
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
            num_ranks, workspace, launch_stream, timeout_ticks, phases,
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
#ifdef MOONCAKE_EP_USE_TENT
    tent_ibgda_transport_ = tent::createIbGdaDeviceTransport();
    auto status = tent_ibgda_transport_->initializeDevice(device_name, 1);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA device initialization failed: "
                   << status.ToString();
        return -1;
    }
    uint32_t lkey = 0;
    uint32_t rkey = 0;
    status = tent_ibgda_transport_->registerMemory(
        gdr_buffer, num_ep_buffer_bytes, lkey, rkey);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA memory registration failed: "
                   << status.ToString();
        return -1;
    }
    gid_index_ = tent_ibgda_transport_->gidIndex();
    LOG(INFO) << "[EP] GPU " << device_id << " uses TENT IBGDA NIC "
              << device_name << " with GID index " << gid_index_;

    // Allocate ctrl_buf without zero-initialization. Individual regions will be
    // initialized as needed: CQ needs -1 (hardware requirement), DBR needs 0.
    // WQ doesn't need initialization as it's zeroed before each use.
    status = tent_ibgda_transport_->allocateControlBuffer(CTRL_BUF_SIZE);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA control buffer allocation failed: "
                   << status.ToString();
        fprintf(stderr,
                "If the error is `Bad address`, probably because your GPU "
                "does not support GPUDirect RDMA.\n");
        (void)tent_ibgda_transport_->unregisterMemory(gdr_buffer);
        return -1;
    }
    ctrl_buf = tent_ibgda_transport_->controlBuffer();
#else
    int num_devices;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    int nic_id = -1;
    for (int i = 0; i < num_devices; ++i) {
        const char* name = ibv_get_device_name(dev_list[i]);
        if (name && device_name == name) {
            nic_id = i;
            break;
        }
    }
    if (nic_id == -1) {
        throw std::runtime_error("Device matching name '" + device_name +
                                 "' not found.");
    }
    LOG(INFO) << "[EP] GPU " << device_id << " uses NIC " << nic_id
              << " out of " << num_devices << " NIC(s)";
    ibv_context* ctx = ibv_open_device(dev_list[nic_id]);
    if (!ctx) {
        perror("Failed to open device");
        return -1;
    }

    // Query port attributes to get GID table length
    ibv_port_attr port_attr;
    const uint8_t port_num = 1;
    if (ibv_query_port(ctx, port_num, &port_attr)) {
        perror("Failed to query port");
        return -1;
    }

    // Dynamically find the best GID index (replaces hardcoded index 3)
    gid_index_ = findBestGidIndex(ctx, port_num, port_attr);
    if (gid_index_ < 0) {
        LOG(ERROR) << "[EP] Failed to find a suitable GID index on "
                   << device_name;
        return -1;
    }

    if (ibv_query_gid(ctx, port_num, gid_index_, &gid)) {
        perror("Failed to query gid");
        return -1;
    }
    ibv_free_device_list(dev_list);

    pd = ibv_alloc_pd(ctx);
    if (!pd) {
        perror("Failed to allocate protection domain");
        return -1;
    }
    mlx5dv_obj dv_obj = {};
    dv_obj.pd.in = pd;
    dv_obj.pd.out = &mpd;
    if (mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD)) {
        perror("Failed to initialize mlx5dv object");
    }
    mr = ibv_reg_mr(pd, gdr_buffer, num_ep_buffer_bytes,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if (!mr) {
        perror("Failed to reg mr");
    }

    // Allocate ctrl_buf without zero-initialization. Individual regions will be
    // initialized as needed: CQ needs -1 (hardware requirement), DBR needs 0.
    // WQ doesn't need initialization as it's zeroed before each use.
    CUDA_CHECK(cudaMalloc(&ctrl_buf, CTRL_BUF_SIZE));
    ctrl_buf_umem = mlx5dv_devx_umem_reg(ctx, ctrl_buf, CTRL_BUF_SIZE,
                                         IBV_ACCESS_LOCAL_WRITE);
    if (!ctrl_buf_umem) {
        perror("Failed to register control buffer as umem");
        fprintf(stderr,
                "If the error is `Bad address`, probably because your GPU "
                "does not support GPUDirect RDMA.\n");
        // Keep internal state consistent: IBGDA init failed, so `mr` must not
        // be treated as valid.
        if (mr) {
            ibv_dereg_mr(mr);
            mr = nullptr;
        }
        return -1;
    }
#endif
#ifdef MOONCAKE_EP_USE_TENT
    status = tent_ibgda_transport_->createQueuePairs(
        USE_QP_COUNT, 16384, comm_stream.stream(), qp_devctxs);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA QP creation failed: "
                   << status.ToString();
        return -1;
    }
    is_roce_ = tent_ibgda_transport_->isRoce();
#else
    ctrl_buf_heap = memheap_create(CTRL_BUF_SIZE);
    if (!ctrl_buf_heap) {
        perror("Failed to create memory heap");
        return -1;
    }
    // Individual regions (CQ, DBR) will be initialized as needed via async
    // memset.
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        mlx5gda_qp* qp =
            mlx5gda_create_rc_qp(mpd, ctrl_buf, ctrl_buf_umem, ctrl_buf_heap,
                                 pd, 16384, 1, comm_stream.stream());
        if (!qp) {
            perror("Failed to create QP");
            return -1;
        }
        is_roce_ = qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
        if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
            perror("Failed to mlx5gda_modify_rc_qp_rst2init");
            return -1;
        }
        // Ensure all async memset operations are complete before accessing QP
        // structures
        CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));

        mlx5gda_qp_devctx qp_devctx = {
            .qpn = qp->qpn,
            .wqeid_mask = qp->num_wqebb - 1,
            .wq = (mlx5gda_wqebb*)(ctrl_buf + qp->wq_offset),
            .cq = (mlx5_cqe64*)(ctrl_buf + qp->send_cq->cq_offset),
            .dbr = (mlx5gda_wq_dbr*)(ctrl_buf + qp->dbr_offset),
            .bf = (char*)qp->uar->reg_addr,
        };
        cudaMemcpy(qp_devctxs + i * sizeof(mlx5gda_qp_devctx), &qp_devctx,
                   sizeof(mlx5gda_qp_devctx), cudaMemcpyHostToDevice);
        qps.push_back(qp);
    }
#endif
    return 0;
#endif  // MOONCAKE_EP_USE_MUSA
}

bool MooncakeEpBuffer::update_local_qpns() {
#ifdef MOONCAKE_EP_USE_MUSA
    return true;
#else
#ifdef MOONCAKE_EP_USE_TENT
    auto status = tent_ibgda_transport_->recreateQueuePairs(
        USE_QP_COUNT, 16384, comm_stream.stream(), qp_devctxs);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA QP recreation failed: "
                   << status.ToString();
        ibgda_disabled_ = true;
        return false;
    }
    is_roce_ = tent_ibgda_transport_->isRoce();
#else
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        if (qps[i]) {
            mlx5gda_destroy_qp(ctrl_buf_heap, qps[i]);
            qps[i] = nullptr;
        }
    }

    for (int i = 0; i < USE_QP_COUNT; ++i) {
        mlx5gda_qp* qp =
            mlx5gda_create_rc_qp(mpd, ctrl_buf, ctrl_buf_umem, ctrl_buf_heap,
                                 pd, 16384, 1, comm_stream.stream());
        if (!qp) {
            perror("Failed to recreate QP");
            ibgda_disabled_ = true;
            return false;
        }
        is_roce_ = qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
        if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
            perror("Failed to mlx5gda_modify_rc_qp_rst2init");
            ibgda_disabled_ = true;
            return false;
        }
        // Ensure all async memset operations are complete before accessing QP
        // structures
        CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));

        mlx5gda_qp_devctx qp_devctx = {
            .qpn = qp->qpn,
            .wqeid_mask = qp->num_wqebb - 1,
            .wq = (mlx5gda_wqebb*)(ctrl_buf + qp->wq_offset),
            .cq = (mlx5_cqe64*)(ctrl_buf + qp->send_cq->cq_offset),
            .dbr = (mlx5gda_wq_dbr*)(ctrl_buf + qp->dbr_offset),
            .bf = (char*)qp->uar->reg_addr,
        };
        cudaMemcpy(qp_devctxs + i * sizeof(mlx5gda_qp_devctx), &qp_devctx,
                   sizeof(mlx5gda_qp_devctx), cudaMemcpyHostToDevice);
        qps[i] = qp;
    }
#endif
    return true;
#endif  // MOONCAKE_EP_USE_MUSA
}

void MooncakeEpBuffer::sync_ib(const std::vector<int64_t>& remote_addrs,
                               const std::vector<int32_t>& remote_keys,
                               const std::vector<int32_t>& remote_qpns,
                               const std::vector<int32_t>& remote_lids,
                               const std::vector<int>& active_ranks_mask) {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no IB support
#else
#ifdef MOONCAKE_EP_USE_TENT
    LOG(FATAL) << "sync_ib legacy path must not be called in TENT mode";
#else
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        int peer_rank = i * num_ranks / USE_QP_COUNT;
        if (active_ranks_mask[peer_rank] == 0) continue;
        ibv_ah_attr ah_attr = {
            .dlid = (uint16_t)remote_lids[i],
            .port_num = 0,
        };
        if (mlx5gda_modify_rc_qp_init2rtr(
                qps[i], ah_attr, (uint32_t)remote_qpns[i], IBV_MTU_4096)) {
            perror("Failed to mlx5gda_modify_rc_qp_init2rtr");
            exit(1);
        }
        if (mlx5gda_modify_rc_qp_rtr2rts(qps[i])) {
            perror("Failed to mlx5gda_modify_rc_qp_rtr2rts");
            exit(1);
        }
    }
    for (int i = 0; i < num_ranks; ++i) {
        if (active_ranks_mask[i] == 0) continue;
        uint64_t raddr =
            i == rank ? (uint64_t)mr->addr : (uint64_t)remote_addrs[i];
        cudaMemcpy(raddrs + i * sizeof(uint64_t), &raddr, sizeof(uint64_t),
                   cudaMemcpyHostToDevice);
        uint32_t rkey = i == rank ? mr->lkey : (uint32_t)remote_keys[i];
        cudaMemcpy(rkeys + i * sizeof(uint32_t), &rkey, sizeof(uint32_t),
                   cudaMemcpyHostToDevice);
    }
#endif
#endif  // MOONCAKE_EP_USE_MUSA
}

void MooncakeEpBuffer::sync_roce(const std::vector<int64_t>& remote_addrs,
                                 const std::vector<int32_t>& remote_keys,
                                 const std::vector<int32_t>& remote_qpns,
                                 const std::vector<int64_t>& subnet_prefixes,
                                 const std::vector<int64_t>& interface_ids,
                                 const std::vector<int>& active_ranks_mask) {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA: no RoCE support
#else
#ifdef MOONCAKE_EP_USE_TENT
    LOG(FATAL) << "sync_roce legacy path must not be called in TENT mode";
#else
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        int peer_rank = i * num_ranks / USE_QP_COUNT;
        if (active_ranks_mask[peer_rank] == 0) continue;
        ibv_gid remote_gid{};
        remote_gid.global.subnet_prefix = subnet_prefixes[peer_rank];
        remote_gid.global.interface_id = interface_ids[peer_rank];
        ibv_ah_attr ah_attr = {};
        ah_attr.is_global = 1;
        ah_attr.grh.dgid = remote_gid;
        ah_attr.grh.sgid_index =
            gid_index_;  // Use dynamically discovered GID index
        ah_attr.grh.hop_limit = 1;
        ah_attr.port_num = 1;
        ah_attr.dlid = qps[i]->port_attr.lid | 0xC000;
        if (mlx5gda_modify_rc_qp_init2rtr(
                qps[i], ah_attr, (uint32_t)remote_qpns[i], IBV_MTU_4096)) {
            perror("Failed to mlx5gda_modify_rc_qp_init2rtr");
            exit(1);
        }
        if (mlx5gda_modify_rc_qp_rtr2rts(qps[i])) {
            perror("Failed to mlx5gda_modify_rc_qp_rtr2rts");
            exit(1);
        }
    }
    for (int i = 0; i < num_ranks; ++i) {
        if (active_ranks_mask[i] == 0) continue;
        uint64_t raddr =
            i == rank ? (uint64_t)mr->addr : (uint64_t)remote_addrs[i];
        cudaMemcpy(raddrs + i * sizeof(uint64_t), &raddr, sizeof(uint64_t),
                   cudaMemcpyHostToDevice);
        uint32_t rkey = i == rank ? mr->lkey : (uint32_t)remote_keys[i];
        cudaMemcpy(rkeys + i * sizeof(uint32_t), &rkey, sizeof(uint32_t),
                   cudaMemcpyHostToDevice);
    }
#endif
#endif  // MOONCAKE_EP_USE_MUSA
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
#ifdef MOONCAKE_EP_USE_TENT
    auto status = tent_ibgda_transport_->connectPeers(
        remote_addrs, remote_keys, peer_qpns, peer_lids, subnet_prefixes,
        interface_ids, active_ranks_mask, rank, num_ranks, raddrs, rkeys);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] TENT IBGDA peer sync failed: "
                   << status.ToString();
        exit(1);
    }
#else
    int all_to_all_size = USE_QP_COUNT / num_ranks;
    std::vector<int32_t> remote_qpns;
    remote_qpns.reserve(USE_QP_COUNT);
    for (int peer_rank = 0; peer_rank < num_ranks; ++peer_rank) {
        int start = rank * all_to_all_size;
        for (int i = 0; i < all_to_all_size; ++i) {
            remote_qpns.push_back(peer_qpns[peer_rank][start + i]);
        }
    }
    if (is_roce_) {
        sync_roce(remote_addrs, remote_keys, remote_qpns, subnet_prefixes,
                  interface_ids, active_ranks_mask);
    } else {
        std::vector<int32_t> remote_lids;
        remote_lids.reserve(USE_QP_COUNT);
        for (int peer_rank = 0; peer_rank < num_ranks; ++peer_rank) {
            int start = rank * all_to_all_size;
            for (int i = 0; i < all_to_all_size; ++i) {
                remote_lids.push_back(peer_lids[peer_rank][start + i]);
            }
        }
        sync_ib(remote_addrs, remote_keys, remote_qpns, remote_lids,
                active_ranks_mask);
    }
#endif
#endif  // MOONCAKE_EP_USE_MUSA
}

std::vector<int32_t> MooncakeEpBuffer::get_ipc_handle() {
    if (use_fabric_mem_) {
        // Fabric memory is globally accessible via cuMemSetAccess — no IPC
        // handle exchange needed. Return an empty vector so the caller knows
        // to skip IPC for this rank.
        return {};
    }
#if defined(MOONCAKE_EP_USE_TENT) && defined(MOONCAKE_EP_USE_MUSA)
    tent::MtLinkIpcHandle handle;
    auto status = tent_mtlink_transport_->exportIpcHandle(gdr_buffer, handle);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to export TENT MTLink IPC handle: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to export TENT MTLink IPC handle");
    }
    return handle.words;
#elif defined(MOONCAKE_EP_USE_TENT)
    tent::NvLinkIpcHandle handle;
    auto status = tent_nvlink_transport_->exportIpcHandle(gdr_buffer, handle);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to export TENT NVLink IPC handle: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to export TENT NVLink IPC handle");
    }
    return handle.words;
#else
    cudaIpcMemHandle_t handle;
    CUDA_CHECK(cudaIpcGetMemHandle(&handle, gdr_buffer));
    // Convert handle bytes to int32_t array
    const size_t handle_size = sizeof(cudaIpcMemHandle_t);
    const size_t num_int32s =
        (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
    std::vector<int32_t> handle_ints(num_int32s);
    memcpy(handle_ints.data(), &handle, handle_size);
    return handle_ints;
#endif
}

void MooncakeEpBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
#if defined(MOONCAKE_EP_USE_TENT) && defined(MOONCAKE_EP_USE_MUSA)
    std::vector<tent::MtLinkIpcHandle> handles(remote_handles.size());
    for (size_t i = 0; i < remote_handles.size(); ++i) {
        handles[i].words = remote_handles[i];
    }
    auto status = tent_mtlink_transport_->configurePeers(
        device_id, gdr_buffer, handles, active_ranks_mask);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to configure TENT MTLink peers: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to configure TENT MTLink peers");
    }
    tent_mtlink_ctx_ = tent_mtlink_transport_->deviceContext();
    nvlink_available = tent_mtlink_ctx_.available;
    ipc_peer_ptrs = tent_mtlink_ctx_.peer_ptrs;
    p2p_ipc_all_enabled_ = tent_mtlink_transport_->allPeersAccessible();
#elif defined(MOONCAKE_EP_USE_TENT)
    std::vector<tent::NvLinkIpcHandle> handles(remote_handles.size());
    for (size_t i = 0; i < remote_handles.size(); ++i) {
        handles[i].words = remote_handles[i];
    }
    auto status = tent_nvlink_transport_->configurePeers(
        device_id, gdr_buffer, handles, active_ranks_mask, use_fabric_mem_);
    if (!status.ok()) {
        LOG(ERROR) << "[EP] Failed to configure TENT NVLink peers: "
                   << std::string(status.message());
        throw std::runtime_error("Failed to configure TENT NVLink peers");
    }
    tent_nvlink_ctx_ = tent_nvlink_transport_->deviceContext();
    nvlink_available = tent_nvlink_ctx_.available;
    ipc_peer_ptrs = tent_nvlink_ctx_.peer_ptrs;
    p2p_ipc_all_enabled_ = tent_nvlink_transport_->allPeersAccessible();
#else
    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));

    std::vector<int32_t> nvlink_array(num_ranks, 0);
    nvlink_array[rank] = 1;

    if (use_fabric_mem_) {
        // MNNVL: fabric addresses are globally visible across the clique.
        // All ranks can directly access each other's gdr_buffer without IPC
        // handle exchange — cuMemSetAccess already granted all devices
        // read/write access during allocation.
        for (int i = 0; i < num_ranks; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            nvlink_array[i] = 1;
            // Each rank's gdr_buffer is directly accessible; the remote
            // addresses will be exchanged via the RDMA address sync path
            // (sync_ib / sync_roce) or via a separate fabric address exchange.
            // For local rank, point to our own buffer.
            ipc_peer_ptrs_host[i] = (i == rank) ? gdr_buffer : nullptr;
        }
        p2p_ipc_all_enabled_ = true;
        LOG(INFO) << "[EP] Fabric memory enabled, skipping IPC handle exchange";
    } else {
        // Non-MNNVL: use cudaIpc for intra-node P2P (original path)
        int node_id = rank / device_count;
        int group_start = node_id * device_count;
        int group_end = std::min(group_start + device_count, num_ranks);

        for (int dst_rank = group_start; dst_rank < group_end; ++dst_rank) {
            if (active_ranks_mask[dst_rank] == 0) continue;
            if (dst_rank == rank) {
                ipc_peer_ptrs_host[dst_rank] = gdr_buffer;
                continue;
            }

            int dst_device = dst_rank % device_count;
            int can_access_peer = 0;
            cudaError_t err = cudaDeviceCanAccessPeer(&can_access_peer,
                                                      device_id, dst_device);
            if (err == cudaSuccess && can_access_peer) {
                cudaError_t peer_err =
                    cudaDeviceEnablePeerAccess(dst_device, 0);
                if (peer_err == cudaSuccess ||
                    peer_err == cudaErrorPeerAccessAlreadyEnabled) {
                    if (peer_err == cudaErrorPeerAccessAlreadyEnabled) {
                        cudaGetLastError();
                    }
                    nvlink_array[dst_rank] = 1;

                    if (dst_rank >= static_cast<int>(remote_handles.size())) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " missing IPC handle for rank " << dst_rank;
                        continue;
                    }

                    const size_t handle_size = sizeof(cudaIpcMemHandle_t);
                    const size_t num_int32s =
                        (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
                    const auto& handle_ints = remote_handles[dst_rank];
                    if (handle_ints.size() < num_int32s) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " invalid IPC handle size for rank " << dst_rank;
                        continue;
                    }

                    cudaIpcMemHandle_t remote_handle;
                    memcpy(&remote_handle, handle_ints.data(), handle_size);

                    void* peer_ptr = nullptr;
                    cudaError_t ipc_err =
                        cudaIpcOpenMemHandle(&peer_ptr, remote_handle,
                                             cudaIpcMemLazyEnablePeerAccess);
                    if (ipc_err != cudaSuccess) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " failed to open IPC handle for rank "
                            << dst_rank << ": " << cudaGetErrorString(ipc_err);
                        nvlink_array[dst_rank] = 0;
                    } else {
                        ipc_peer_ptrs_host[dst_rank] = peer_ptr;
                    }
                }
            }
        }

        p2p_ipc_all_enabled_ = true;
        for (int i = 0; i < num_ranks; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            if (nvlink_array[i] == 0 || ipc_peer_ptrs_host[i] == nullptr) {
                p2p_ipc_all_enabled_ = false;
                break;
            }
        }
        if (p2p_ipc_all_enabled_ && num_ranks > 1) {
            int first_node_id = 0 / device_count;
            int last_node_id = (num_ranks - 1) / device_count;
            if (first_node_id != last_node_id) {
                p2p_ipc_all_enabled_ = false;
            }
        }
    }

    CUDA_CHECK(cudaMemcpy(nvlink_available, nvlink_array.data(),
                          num_ranks * sizeof(int32_t), cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(ipc_peer_ptrs, ipc_peer_ptrs_host,
                          num_ranks * sizeof(void*), cudaMemcpyHostToDevice));
#endif
}

}  // namespace mooncake
