#include <fstream>
#include <pybind11/functional.h>
#include <torch/python.h>

#include "api.cuh"
#include "event.hpp"
#include "exception.cuh"
#include "memheap.h"
#include "mlx5gda.h"

#ifndef TORCH_EXTENSION_NAME
#define TORCH_EXTENSION_NAME mxa_ep_cpp
#endif

namespace mxa_ep {

struct BufferLayout {
    int* rdma_send_signal_buffer;
    int* rdma_recv_signal_buffer;
    void* rdma_send_data_buffer;
    void* rdma_recv_data_buffer;

    void* cuda_counter_buffer;
    void* cuda_data_buffer;
};

struct BufferPair {
    size_t total_bytes = 0;
    BufferLayout buffers[2];

    template <typename out_ptr_t = void*, typename count_ptr_t = uint8_t*,
              typename in_ptr_t = void*>
    static out_ptr_t advance(const in_ptr_t& ptr, size_t count) {
        return reinterpret_cast<out_ptr_t>(reinterpret_cast<count_ptr_t>(ptr) +
                                           count);
    }

    BufferPair(void* rdma_buffer, int num_max_dispatch_tokens_per_rank,
               int hidden, int num_ranks, int num_experts,
               size_t bytes_reserved) {
        total_bytes = bytes_reserved;
        size_t signaling_buffer_bytes = num_experts * sizeof(int);
        size_t send_recv_buffer_bytes =
            num_experts * num_max_dispatch_tokens_per_rank *
            (2 * sizeof(int4) + hidden * sizeof(nv_bfloat16));
        for (int i = 0; i < 2; ++i) {
            size_t rdma_base_offset = total_bytes +
                                      2 * i * signaling_buffer_bytes +
                                      2 * i * send_recv_buffer_bytes;
            buffers[i] = {
                advance<int*>(rdma_buffer, rdma_base_offset),
                advance<int*>(rdma_buffer,
                              rdma_base_offset + signaling_buffer_bytes),
                advance<int*>(rdma_buffer,
                              rdma_base_offset + 2 * signaling_buffer_bytes),
                advance<int*>(rdma_buffer, rdma_base_offset +
                                               2 * signaling_buffer_bytes +
                                               send_recv_buffer_bytes),
                advance<void*>(rdma_buffer, rdma_base_offset +
                                                2 * signaling_buffer_bytes +
                                                2 * send_recv_buffer_bytes),
                advance<void*>(rdma_buffer, rdma_base_offset +
                                                3 * signaling_buffer_bytes +
                                                2 * send_recv_buffer_bytes),
            };
        }
        total_bytes += 4 * signaling_buffer_bytes + 4 * send_recv_buffer_bytes;
    }
};

struct Buffer {
   private:
    // Device info and communication
    int device_id;
    int rank, num_ranks;
    int clock_rate_khz;

    // MXA Buffer
    int buffer_idx{};
    int64_t num_mxa_bytes;
    void* gdr_buffer = nullptr;
    size_t bytes_reserved;  // For all-reduce

    // IBGDA
    const size_t ctrl_buf_size = 256 * 1024 * 1024;  // 256 MiB
    void* ctrl_buf = nullptr;
    ibv_mr* mr;
    std::vector<mlx5gda_qp*> qps;
    ibv_gid gid;
    void* raddrs = nullptr;
    void* rkeys = nullptr;
    void* qp_devctxs = nullptr;

    // Stream for communication
    at::cuda::CUDAStream comm_stream;

    // Workspace
    void* workspace = nullptr;

   public:
    Buffer(int rank, int num_ranks, int64_t num_mxa_bytes,
           size_t bytes_reserved)
        : rank(rank),
          num_ranks(num_ranks),
          num_mxa_bytes(num_mxa_bytes),
          bytes_reserved(bytes_reserved),
          comm_stream(at::cuda::getStreamFromPool(true)) {
        // Get ranks
        CUDA_CHECK(cudaGetDevice(&device_id));
        CUDA_CHECK(cudaDeviceGetAttribute(&clock_rate_khz, cudaDevAttrClockRate,
                                          device_id));
        CUDA_CHECK(cudaMalloc(&gdr_buffer, num_mxa_bytes));
        CUDA_CHECK(cudaMalloc(&raddrs, num_ranks * sizeof(uint64_t)));
        CUDA_CHECK(cudaMalloc(&rkeys, num_ranks * sizeof(uint32_t)));
        CUDA_CHECK(
            cudaMalloc(&qp_devctxs, num_ranks * sizeof(mlx5gda_qp_devctx)));
        init_ibgda();

        // Create 32 MiB workspace
        CUDA_CHECK(cudaMalloc(&workspace, NUM_WORKSPACE_BYTES));
        CUDA_CHECK(
            cudaMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));
    }

    ~Buffer() noexcept(false) {
        cudaFree(gdr_buffer);
        cudaFree(raddrs);
        cudaFree(rkeys);
        cudaFree(qp_devctxs);
    }

    std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
               torch::Tensor, torch::Tensor, std::optional<EventHandle>,
               std::optional<std::function<void()>>>
    dispatch(const torch::Tensor& x, const torch::Tensor& topk_idx,
             torch::Tensor& broken_nodes, int num_max_dispatch_tokens_per_rank,
             int num_experts, int timeout_us, bool use_fp8, bool async,
             bool return_recv_hook) {
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

        auto num_tokens = static_cast<int>(x.size(0)),
             hidden = static_cast<int>(x.size(1));
        auto num_scales = hidden / 128,
             num_topk = static_cast<int>(topk_idx.size(1));
        int num_local_experts = num_experts / num_ranks;

        // Buffer control
        BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                          num_ranks, num_experts, bytes_reserved);
        EP_HOST_ASSERT(layout.total_bytes <= num_mxa_bytes + bytes_reserved);
        auto buffer = layout.buffers[buffer_idx];
        auto next_buffer = layout.buffers[buffer_idx ^= 1];

        // Wait previous tasks to be finished
        // NOTES: the hook mode will always use the default stream
        auto compute_stream = at::cuda::getCurrentCUDAStream();
        auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
        EP_HOST_ASSERT(not(async and return_recv_hook));
        if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

        // Allocate packed tensors
        auto packed_recv_x =
            torch::empty({num_local_experts,
                          num_ranks * num_max_dispatch_tokens_per_rank, hidden},
                         x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn
                                                   : torch::kBFloat16));
        auto packed_recv_src_info = torch::empty(
            {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank},
            torch::dtype(torch::kInt32).device(torch::kCUDA));
        auto packed_recv_layout_range =
            torch::empty({num_local_experts, num_ranks},
                         torch::dtype(torch::kInt64).device(torch::kCUDA));
        auto packed_recv_count =
            torch::zeros({num_local_experts},
                         torch::dtype(torch::kInt32).device(torch::kCUDA));

        // Allocate column-majored scales
        auto packed_recv_x_scales = std::optional<torch::Tensor>();
        float* packed_recv_x_scales_ptr = nullptr;
        if (use_fp8) {
            EP_HOST_ASSERT(
                (num_ranks * num_max_dispatch_tokens_per_rank) % 4 == 0 and
                "TMA requires the number of tokens to be multiple of 4");
            packed_recv_x_scales = torch::empty(
                {num_local_experts, num_scales,
                 num_ranks * num_max_dispatch_tokens_per_rank},
                torch::dtype(torch::kFloat32).device(torch::kCUDA));
            packed_recv_x_scales =
                torch::transpose(packed_recv_x_scales.value(), 1, 2);
            packed_recv_x_scales_ptr = packed_recv_x_scales->data_ptr<float>();
        }

        int64_t timeout_ticks =
            timeout_us == -1
                ? -1
                : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

        auto launcher = [=](int phases) {
            cudaMemsetAsync(buffer.cuda_counter_buffer, 0,
                            num_experts * sizeof(int), launch_stream);
            mxa_ep::dispatch(
                packed_recv_x.data_ptr(), packed_recv_x_scales_ptr,
                packed_recv_src_info.data_ptr<int>(),
                packed_recv_layout_range.data_ptr<int64_t>(),
                packed_recv_count.data_ptr<int>(),
                broken_nodes.data_ptr<int32_t>(), gdr_buffer,
                buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
                buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer,
                buffer.cuda_counter_buffer, buffer.cuda_data_buffer, raddrs,
                rkeys, qp_devctxs, x.data_ptr(), topk_idx.data_ptr<int64_t>(),
                next_buffer.rdma_recv_signal_buffer, num_tokens, hidden,
                num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
                num_ranks, use_fp8, workspace, launch_stream, timeout_ticks,
                phases);
        };
        launcher(return_recv_hook
                     ? LOW_LATENCY_SEND_PHASE
                     : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));

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
        if (return_recv_hook)
            recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };

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
    combine(const torch::Tensor& x, const torch::Tensor& topk_idx,
            const torch::Tensor& topk_weights, const torch::Tensor& src_info,
            const torch::Tensor& layout_range, torch::Tensor& gathered_experts,
            int num_max_dispatch_tokens_per_rank, int num_experts,
            int timeout_us, bool zero_copy, bool async, bool return_recv_hook,
            const std::optional<torch::Tensor>& out) {
        // Tensor checks
        EP_HOST_ASSERT(x.dim() == 3 and x.is_contiguous() and
                       x.scalar_type() == torch::kBFloat16);
        EP_HOST_ASSERT(x.size(0) == num_experts / num_ranks);
        EP_HOST_ASSERT(x.size(1) ==
                       num_ranks * num_max_dispatch_tokens_per_rank);
        EP_HOST_ASSERT(x.size(2) % sizeof(int4) == 0 and x.size(2) % 128 == 0);
        EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
        EP_HOST_ASSERT(topk_idx.size(0) == topk_weights.size(0) and
                       topk_idx.size(1) == topk_weights.size(1));
        EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
        EP_HOST_ASSERT(topk_weights.dim() == 2 and
                       topk_weights.is_contiguous());
        EP_HOST_ASSERT(topk_weights.size(0) <=
                       num_max_dispatch_tokens_per_rank);
        EP_HOST_ASSERT(topk_weights.scalar_type() == torch::kFloat32);
        EP_HOST_ASSERT(src_info.dim() == 2 and src_info.is_contiguous());
        EP_HOST_ASSERT(src_info.scalar_type() == torch::kInt32 and
                       x.size(0) == src_info.size(0));
        EP_HOST_ASSERT(layout_range.dim() == 2 and
                       layout_range.is_contiguous());
        EP_HOST_ASSERT(layout_range.scalar_type() == torch::kInt64);
        EP_HOST_ASSERT(layout_range.size(0) == num_experts / num_ranks and
                       layout_range.size(1) == num_ranks);
        auto hidden = static_cast<int>(x.size(2));
        auto num_local_experts = num_experts / num_ranks,
             num_topk = static_cast<int>(topk_weights.size(1));
        auto num_combined_tokens = static_cast<int>(topk_weights.size(0));

        // Buffer control
        BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                          num_ranks, num_experts, bytes_reserved);
        EP_HOST_ASSERT(layout.total_bytes <= num_mxa_bytes + bytes_reserved);
        auto buffer = layout.buffers[buffer_idx];
        auto next_buffer = layout.buffers[buffer_idx ^= 1];

        // Wait previous tasks to be finished
        // NOTES: the hook mode will always use the default stream
        auto compute_stream = at::cuda::getCurrentCUDAStream();
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
            combined_x =
                torch::empty({num_combined_tokens, hidden}, x.options());
        }

        int64_t timeout_ticks =
            timeout_us == -1
                ? -1
                : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

        // Kernel launch
        auto launcher = [=](int phases) {
            mxa_ep::combine(
                combined_x.data_ptr(), gathered_experts.data_ptr<int32_t>(),
                gdr_buffer, buffer.rdma_send_signal_buffer,
                buffer.rdma_recv_signal_buffer, buffer.rdma_send_data_buffer,
                buffer.rdma_recv_data_buffer, buffer.cuda_counter_buffer,
                buffer.cuda_data_buffer, raddrs, rkeys, qp_devctxs,
                x.data_ptr(), topk_idx.data_ptr<int64_t>(),
                topk_weights.data_ptr<float>(), src_info.data_ptr<int>(),
                layout_range.data_ptr<int64_t>(),
                next_buffer.rdma_recv_signal_buffer, num_combined_tokens,
                hidden, num_max_dispatch_tokens_per_rank, num_topk, num_experts,
                rank, num_ranks, workspace, launch_stream, timeout_ticks,
                phases, zero_copy);
        };
        launcher(return_recv_hook
                     ? LOW_LATENCY_SEND_PHASE
                     : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));

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
        if (return_recv_hook)
            recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };

        // Return values
        return {combined_x, event, recv_hook};
    }

    torch::Tensor get_next_combine_buffer(int num_max_dispatch_tokens_per_rank,
                                          int hidden, int num_experts) {
        BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                          num_ranks, num_experts, bytes_reserved);

        auto buffer = layout.buffers[buffer_idx];
        auto dtype = torch::kBFloat16;
        size_t num_bytes_per_combine_msg = hidden * sizeof(nv_bfloat16);
        auto num_msg_elems = static_cast<int>(num_bytes_per_combine_msg /
                                              elementSize(torch::kBFloat16));

        EP_HOST_ASSERT(
            num_bytes_per_combine_msg % elementSize(torch::kBFloat16) == 0);
        return torch::from_blob(
            buffer.rdma_send_data_buffer,
            {num_experts / num_ranks,
             num_ranks * num_max_dispatch_tokens_per_rank, hidden},
            {num_ranks * num_max_dispatch_tokens_per_rank * num_msg_elems,
             num_msg_elems, 1},
            torch::TensorOptions().dtype(dtype).device(torch::kCUDA));
    }

    void all_reduce_without(const torch::Tensor& broken_nodes,
                            torch::Tensor& x) {
        auto mxa_buffer = reinterpret_cast<int*>(gdr_buffer);
        int size = x.numel();
        cudaStream_t stream = at::cuda::getCurrentCUDAStream();
        cudaMemsetAsync(mxa_buffer, 0, (1 + size) * num_ranks * sizeof(int),
                        stream);
        mxa_ep::all_reduce_without(broken_nodes.data_ptr<int32_t>(),
                                   x.data_ptr<int>(), mxa_buffer, raddrs, rkeys,
                                   qp_devctxs, size, rank, num_ranks, stream);
    }

    void init_ibgda() {
        std::ifstream config("gpu_to_nic.txt");
        if (!config)
            throw std::runtime_error("Cannot open config file gpu_to_nic.txt");
        std::unordered_map<int, int> gpu_to_nic;
        std::string line;
        for (size_t lineno = 1; std::getline(config, line); ++lineno) {
            // Strip everything after ‘#’
            if (auto pos = line.find('#'); pos != std::string::npos)
                line.erase(pos);
            std::istringstream iss(line);
            int gpu, nic;
            if (!(iss >> gpu >> nic)) {
                if (iss.rdbuf()->in_avail() == 0)
                    continue;  // blank or comment line
                throw std::runtime_error("Parse error in gpu_to_nic.txt line " +
                                         std::to_string(lineno));
            }
            if (!gpu_to_nic.emplace(gpu, nic).second)
                throw std::runtime_error("Duplicate GPU id on line " +
                                         std::to_string(lineno));
        }

        auto nic = gpu_to_nic.find(device_id);
        if (nic == gpu_to_nic.end())
            throw std::out_of_range("GPU id not found in config");
        int num_devices;
        ibv_device** dev_list = ibv_get_device_list(&num_devices);
        printf("GPU %d uses NIC %d out of %d NIC(s)\n", device_id, nic->second,
               num_devices);
        ibv_context* ctx = ibv_open_device(dev_list[nic->second]);
        if (!ctx) {
            perror("Failed to open device");
            exit(1);
        }
        if (ibv_query_gid(ctx, 1, 3, &gid)) {
            perror("Failed to query gid");
        }
        ibv_free_device_list(dev_list);

        ibv_pd* pd = ibv_alloc_pd(ctx);
        if (!pd) {
            perror("Failed to allocate protection domain");
            exit(1);
        }
        mlx5dv_pd mpd;
        mlx5dv_obj dv_obj = {};
        dv_obj.pd.in = pd;
        dv_obj.pd.out = &mpd;
        if (mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD)) {
            perror("Failed to initialize mlx5dv object");
        }
        mr = ibv_reg_mr(pd, gdr_buffer, num_mxa_bytes,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
        if (!mr) {
            perror("Failed to reg mr");
        }

        CUDA_CHECK(cudaMalloc(&ctrl_buf, ctrl_buf_size));
        CUDA_CHECK(cudaMemset(ctrl_buf, 0, ctrl_buf_size));
        mlx5dv_devx_umem* ctrl_buf_umem = mlx5dv_devx_umem_reg(
            ctx, ctrl_buf, ctrl_buf_size, IBV_ACCESS_LOCAL_WRITE);
        if (!ctrl_buf_umem) {
            perror("Failed to register control buffer as umem");
            fprintf(stderr,
                    "If the error is `Bad address`, probably because your GPU "
                    "does not support GPUDirect RDMA.\n");
            exit(1);
        }
        memheap* ctrl_buf_heap = memheap_create(ctrl_buf_size);
        if (!ctrl_buf_heap) {
            perror("Failed to create memory heap");
            exit(1);
        }
        for (int i = 0; i < num_ranks; ++i) {
            mlx5gda_qp* qp = mlx5gda_create_rc_qp(mpd, ctrl_buf, ctrl_buf_umem,
                                                  ctrl_buf_heap, pd, 16384, 1);
            if (!qp) {
                perror("Failed to create QP");
                exit(1);
            }
            if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
                perror("Failed to mlx5gda_modify_rc_qp_rst2init");
                exit(1);
            }
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
    }

    void sync(const std::vector<int64_t>& remote_addrs,
              const std::vector<int32_t>& remote_keys,
              const std::vector<int32_t>& remote_qpns,
              const std::vector<int64_t>& subnet_prefixes,
              const std::vector<int64_t>& interface_ids) {
        for (int i = 0; i < num_ranks; ++i) {
            ibv_gid remote_gid{};
            remote_gid.global.subnet_prefix = subnet_prefixes[i];
            remote_gid.global.interface_id = interface_ids[i];
            ibv_ah_attr ah_attr = {};
            ah_attr.is_global = 1;
            ah_attr.grh.dgid = remote_gid;
            ah_attr.grh.sgid_index = 3;
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
            uint64_t raddr =
                i == rank ? (uint64_t)mr->addr : (uint64_t)remote_addrs[i];
            cudaMemcpy(raddrs + i * sizeof(uint64_t), &raddr, sizeof(uint64_t),
                       cudaMemcpyHostToDevice);
            uint32_t rkey = i == rank ? mr->lkey : (uint32_t)remote_keys[i];
            cudaMemcpy(rkeys + i * sizeof(uint32_t), &rkey, sizeof(uint32_t),
                       cudaMemcpyHostToDevice);
        }
    }

    std::tuple<int64_t, int32_t> get_mr_info() {
        return {(int64_t)mr->addr, (int32_t)mr->rkey};
    }

    std::tuple<int64_t, int64_t> get_gid() {
        return {(int64_t)gid.global.subnet_prefix,
                (int64_t)gid.global.interface_id};
    }

    std::vector<torch::Tensor> get_local_qpns() {
        std::vector<torch::Tensor> local_qpns;
        for (int i = 0; i < num_ranks; ++i) {
            local_qpns.push_back(
                torch::full({1}, qps[i]->qpn,
                            torch::dtype(torch::kInt32).device(torch::kCUDA)));
        }
        return local_qpns;
    }

    std::vector<torch::Tensor> get_local_lids() {
        std::vector<torch::Tensor> local_lids;
        for (int i = 0; i < num_ranks; ++i) {
            local_lids.push_back(
                torch::full({1}, qps[i]->port_attr.lid,
                            torch::dtype(torch::kInt32).device(torch::kCUDA)));
        }
        return local_lids;
    }
};

size_t get_mxa_size_hint(int num_max_dispatch_tokens_per_rank, int hidden,
                         int num_ranks, int num_experts,
                         size_t bytes_reserved) {
    return BufferPair(nullptr, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts, bytes_reserved)
        .total_bytes;
}

}  // namespace mxa_ep

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.doc() = "MXA-EP: Expert parallelism with MXA";

    m.def("get_mxa_size_hint", &mxa_ep::get_mxa_size_hint);

    pybind11::class_<mxa_ep::EventHandle>(m, "EventHandle")
        .def(pybind11::init<>())
        .def("current_stream_wait", &mxa_ep::EventHandle::current_stream_wait);

    pybind11::class_<mxa_ep::Buffer>(m, "Buffer")
        .def(pybind11::init<int, int, int64_t, size_t>())
        .def("sync", &mxa_ep::Buffer::sync)
        .def("get_mr_info", &mxa_ep::Buffer::get_mr_info)
        .def("get_gid", &mxa_ep::Buffer::get_gid)
        .def("get_local_qpns", &mxa_ep::Buffer::get_local_qpns)
        .def("get_local_lids", &mxa_ep::Buffer::get_local_lids)
        .def("all_reduce_without", &mxa_ep::Buffer::all_reduce_without)
        .def("dispatch", &mxa_ep::Buffer::dispatch)
        .def("combine", &mxa_ep::Buffer::combine)
        .def("get_next_combine_buffer",
             &mxa_ep::Buffer::get_next_combine_buffer);
}
