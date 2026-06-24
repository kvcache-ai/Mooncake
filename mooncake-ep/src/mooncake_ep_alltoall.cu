#include <mooncake_ep_alltoall.h>

#include <ATen/cuda/CUDAContext.h>
#include <cuda_bf16.h>
#include <cuda_runtime.h>

#include <c10/cuda/CUDAGuard.h>

namespace mooncake {
namespace {

inline void check_cuda(const torch::Tensor& t, const char* name) {
    TORCH_CHECK(t.is_cuda(), name, " must be a CUDA tensor");
    TORCH_CHECK(t.is_contiguous(), name, " must be contiguous");
}

inline int ceil_div(int a, int b) { return (a + b - 1) / b; }

__device__ __forceinline__ void store_i32_words(nv_bfloat16* dst,
                                                int32_t value) {
    auto* words = reinterpret_cast<uint16_t*>(dst);
    uint32_t raw = static_cast<uint32_t>(value);
    words[0] = static_cast<uint16_t>(raw & 0xffffu);
    words[1] = static_cast<uint16_t>(raw >> 16);
}

__device__ __forceinline__ int32_t load_i32_words(const nv_bfloat16* src) {
    const auto* words = reinterpret_cast<const uint16_t*>(src);
    uint32_t raw = static_cast<uint32_t>(words[0]) |
                   (static_cast<uint32_t>(words[1]) << 16);
    return static_cast<int32_t>(raw);
}

__global__ void count_dispatch_kernel(const int64_t* topk_idx,
                                      int32_t* counts_by_expert, int num_tokens,
                                      int num_topk, int num_local_experts) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    int total = num_tokens * num_topk;
    if (i >= total) return;

    int64_t expert = topk_idx[i];
    if (expert < 0) return;
    int dst_rank = static_cast<int>(expert / num_local_experts);
    int local_expert = static_cast<int>(expert % num_local_experts);
    atomicAdd(counts_by_expert + dst_rank * num_local_experts + local_expert,
              1);
}

__global__ void prefix_counts_kernel(const int32_t* counts_by_expert,
                                     int32_t* expert_offsets, int num_ranks,
                                     int num_local_experts) {
    int r = blockIdx.x;
    if (r >= num_ranks || threadIdx.x != 0) return;
    int running = 0;
    for (int e = 0; e < num_local_experts; ++e) {
        expert_offsets[r * num_local_experts + e] = running;
        running += counts_by_expert[r * num_local_experts + e];
    }
}

__global__ void pack_dispatch_fused_kernel(
    const nv_bfloat16* x, const int64_t* topk_idx,
    const int32_t* expert_offsets, int32_t* counters, nv_bfloat16* send_payload,
    int64_t* send_route, int num_tokens, int hidden, int num_topk,
    int num_ranks, int num_local_experts, int max_messages_per_rank) {
    int i = blockIdx.x;
    int tid = threadIdx.x;
    int total = num_tokens * num_topk;
    if (i >= total) return;

    int64_t expert = topk_idx[i];
    if (expert < 0) return;

    int token = i / num_topk;
    int slot = i - token * num_topk;
    int dst_rank = static_cast<int>(expert / num_local_experts);
    int local_expert = static_cast<int>(expert % num_local_experts);
    int group = dst_rank * num_local_experts + local_expert;
    __shared__ int block_local_pos;
    if (tid == 0) block_local_pos = atomicAdd(counters + group, 1);
    __syncthreads();
    int local_pos = block_local_pos;
    int pos = expert_offsets[group] + local_pos;
    if (pos >= max_messages_per_rank) return;

    int fused_hidden = hidden + 2;
    int fused_slots_per_rank = max_messages_per_rank + num_local_experts;
    nv_bfloat16* dst = send_payload + (dst_rank * fused_slots_per_rank +
                                       num_local_experts + pos) *
                                          fused_hidden;
    const nv_bfloat16* src = x + token * hidden;
    for (int h = tid; h < hidden; h += blockDim.x) dst[h] = src[h];
    if (tid == 0) {
        store_i32_words(dst + hidden, slot * num_tokens + token);
        int route_idx = i * 4;
        send_route[route_idx + 0] = dst_rank;
        send_route[route_idx + 1] = pos;
        send_route[route_idx + 2] = token;
        send_route[route_idx + 3] = slot;
    }
}

__global__ void append_counts_to_payload_kernel(
    const int32_t* counts_by_expert, nv_bfloat16* send_payload, int num_ranks,
    int num_local_experts, int max_messages_per_rank, int hidden) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    int total = num_ranks * num_local_experts;
    if (i >= total) return;
    int dst_rank = i / num_local_experts;
    int local_expert = i - dst_rank * num_local_experts;
    int fused_hidden = hidden + 2;
    int fused_slots_per_rank = max_messages_per_rank + num_local_experts;
    nv_bfloat16* dst =
        send_payload +
        (dst_rank * fused_slots_per_rank + local_expert) * fused_hidden;
    store_i32_words(dst, counts_by_expert[i]);
}

__global__ void compact_dispatch_fused_kernel(
    const nv_bfloat16* recv_payload, int64_t* layout_range, int32_t* recv_count,
    int64_t* return_src_pos, nv_bfloat16* packed_recv_x,
    int32_t* packed_recv_src_info, int hidden, int num_ranks,
    int num_local_experts, int max_messages_per_rank, int num_recv_slots) {
    int idx = blockIdx.x;
    int m = idx % max_messages_per_rank;
    int pair = idx / max_messages_per_rank;
    int src_rank = pair / num_local_experts;
    int local_expert = pair - src_rank * num_local_experts;
    if (src_rank >= num_ranks) return;

    int fused_hidden = hidden + 2;
    int fused_slots_per_rank = max_messages_per_rank + num_local_experts;
    const nv_bfloat16* rank_base =
        recv_payload + src_rank * fused_slots_per_rank * fused_hidden;
    int count = load_i32_words(rank_base + local_expert * fused_hidden);

    int begin = 0;
    for (int r = 0; r < src_rank; ++r) {
        const nv_bfloat16* prev_rank_base =
            recv_payload + r * fused_slots_per_rank * fused_hidden;
        begin += load_i32_words(prev_rank_base + local_expert * fused_hidden);
    }

    if (m == 0 && threadIdx.x == 0) {
        layout_range[local_expert * num_ranks + src_rank] =
            (static_cast<int64_t>(begin) << 32) | static_cast<uint32_t>(count);
        atomicAdd(recv_count + local_expert, count);
    }

    if (m >= count) return;

    int src_begin = 0;
    for (int e = 0; e < local_expert; ++e)
        src_begin += load_i32_words(rank_base + e * fused_hidden);

    int dst_pos = begin + m;
    int src_pos = src_begin + m;
    if (dst_pos >= num_recv_slots || src_pos >= max_messages_per_rank) return;

    const nv_bfloat16* src =
        rank_base + (num_local_experts + src_pos) * fused_hidden;
    nv_bfloat16* dst =
        packed_recv_x + (local_expert * num_recv_slots + dst_pos) * hidden;
    for (int h = threadIdx.x; h < hidden; h += blockDim.x) dst[h] = src[h];
    if (threadIdx.x == 0) {
        packed_recv_src_info[local_expert * num_recv_slots + dst_pos] =
            load_i32_words(src + hidden);
        int route_idx = (local_expert * num_recv_slots + dst_pos) * 2;
        return_src_pos[route_idx + 0] = src_rank;
        return_src_pos[route_idx + 1] = src_pos;
    }
}

__global__ void pack_combine_kernel(const nv_bfloat16* expert_buffers,
                                    const int64_t* return_src_pos,
                                    nv_bfloat16* send_payload, int hidden,
                                    int max_messages_per_rank,
                                    int num_recv_slots) {
    int idx = blockIdx.x;
    int tid = threadIdx.x;
    int local_expert = idx / num_recv_slots;
    int expert_pos = idx - local_expert * num_recv_slots;
    int route_idx = idx * 2;
    int64_t dst_rank = return_src_pos[route_idx + 0];
    int64_t dst_pos = return_src_pos[route_idx + 1];
    if (dst_rank < 0 || dst_pos < 0) return;

    nv_bfloat16* dst =
        send_payload + (dst_rank * max_messages_per_rank + dst_pos) * hidden;
    const nv_bfloat16* src =
        expert_buffers + (local_expert * num_recv_slots + expert_pos) * hidden;
    for (int h = tid; h < hidden; h += blockDim.x) dst[h] = src[h];
}

__global__ void reduce_combine_kernel(const nv_bfloat16* recv_payload,
                                      const int64_t* send_route,
                                      const float* topk_weights,
                                      nv_bfloat16* combined, int num_tokens,
                                      int hidden, int num_topk,
                                      int max_messages_per_rank) {
    int token = blockIdx.x;
    int h = blockIdx.y * blockDim.x + threadIdx.x;
    if (token >= num_tokens || h >= hidden) return;

    float acc = 0.0f;
    for (int slot = 0; slot < num_topk; ++slot) {
        int route = token * num_topk + slot;
        int64_t rank = send_route[route * 4 + 0];
        int64_t pos = send_route[route * 4 + 1];
        if (rank < 0 || pos < 0) continue;
        const nv_bfloat16* src =
            recv_payload + (rank * max_messages_per_rank + pos) * hidden;
        float weight = topk_weights[route];
        acc += __bfloat162float(src[h]) * weight;
    }
    combined[token * hidden + h] = __float2bfloat16(acc);
}

}  // namespace

std::tuple<torch::Tensor, torch::Tensor> torch_alltoall_pack_dispatch_fused(
    const torch::Tensor& x, const torch::Tensor& topk_idx, int num_experts,
    int num_ranks) {
    check_cuda(x, "x");
    check_cuda(topk_idx, "topk_idx");
    TORCH_CHECK(x.scalar_type() == torch::kBFloat16, "x must be bfloat16");
    TORCH_CHECK(topk_idx.scalar_type() == torch::kInt64,
                "topk_idx must be int64");
    TORCH_CHECK(x.dim() == 2 && topk_idx.dim() == 2,
                "x/topk_idx must be 2D tensors");
    TORCH_CHECK(x.size(0) == topk_idx.size(0),
                "x and topk_idx must have the same token count");
    TORCH_CHECK(num_ranks > 0, "num_ranks must be positive");
    TORCH_CHECK(num_experts > 0 && num_experts % num_ranks == 0,
                "num_experts must be positive and divisible by num_ranks");

    const c10::cuda::CUDAGuard guard(x.device());
    auto stream = at::cuda::getCurrentCUDAStream();
    int num_tokens = static_cast<int>(x.size(0));
    int hidden = static_cast<int>(x.size(1));
    int num_topk = static_cast<int>(topk_idx.size(1));
    int num_local_experts = num_experts / num_ranks;
    int max_messages_per_rank = num_tokens * num_topk;
    int fused_hidden = hidden + 2;
    int fused_slots_per_rank = max_messages_per_rank + num_local_experts;
    auto int32_opts =
        torch::TensorOptions().dtype(torch::kInt32).device(x.device());
    auto int64_opts =
        torch::TensorOptions().dtype(torch::kInt64).device(x.device());

    auto counts_by_expert =
        torch::zeros({num_ranks, num_local_experts}, int32_opts);
    int total = num_tokens * num_topk;
    count_dispatch_kernel<<<ceil_div(total, 256), 256, 0, stream>>>(
        topk_idx.data_ptr<int64_t>(), counts_by_expert.data_ptr<int32_t>(),
        num_tokens, num_topk, num_local_experts);

    auto expert_offsets =
        torch::empty({num_ranks, num_local_experts}, int32_opts);
    prefix_counts_kernel<<<num_ranks, 1, 0, stream>>>(
        counts_by_expert.data_ptr<int32_t>(),
        expert_offsets.data_ptr<int32_t>(), num_ranks, num_local_experts);

    auto send_payload = torch::zeros(
        {num_ranks, fused_slots_per_rank, fused_hidden}, x.options());
    auto send_route = torch::full({total, 4}, -1, int64_opts);
    auto counters = torch::zeros({num_ranks, num_local_experts}, int32_opts);
    pack_dispatch_fused_kernel<<<total, 256, 0, stream>>>(
        reinterpret_cast<const nv_bfloat16*>(x.data_ptr()),
        topk_idx.data_ptr<int64_t>(), expert_offsets.data_ptr<int32_t>(),
        counters.data_ptr<int32_t>(),
        reinterpret_cast<nv_bfloat16*>(send_payload.data_ptr()),
        send_route.data_ptr<int64_t>(), num_tokens, hidden, num_topk, num_ranks,
        num_local_experts, max_messages_per_rank);
    append_counts_to_payload_kernel<<<
        ceil_div(num_ranks * num_local_experts, 256), 256, 0, stream>>>(
        counts_by_expert.data_ptr<int32_t>(),
        reinterpret_cast<nv_bfloat16*>(send_payload.data_ptr()), num_ranks,
        num_local_experts, max_messages_per_rank, hidden);
    return {send_payload, send_route};
}

std::tuple<torch::Tensor, torch::Tensor, torch::Tensor, torch::Tensor,
           torch::Tensor>
torch_alltoall_compact_dispatch_fused(const torch::Tensor& recv_payload,
                                      int num_local_experts,
                                      int num_max_dispatch_tokens_per_rank) {
    check_cuda(recv_payload, "recv_payload");
    TORCH_CHECK(recv_payload.scalar_type() == torch::kBFloat16,
                "recv_payload must be bfloat16");
    TORCH_CHECK(recv_payload.dim() == 3, "recv_payload must be a 3D tensor");
    TORCH_CHECK(num_local_experts > 0, "num_local_experts must be positive");
    TORCH_CHECK(num_max_dispatch_tokens_per_rank > 0,
                "num_max_dispatch_tokens_per_rank must be positive");
    const c10::cuda::CUDAGuard guard(recv_payload.device());
    auto stream = at::cuda::getCurrentCUDAStream();
    int num_ranks = static_cast<int>(recv_payload.size(0));
    int fused_slots_per_rank = static_cast<int>(recv_payload.size(1));
    int fused_hidden = static_cast<int>(recv_payload.size(2));
    TORCH_CHECK(fused_hidden > 2, "recv_payload hidden dimension is invalid");
    TORCH_CHECK(fused_slots_per_rank > num_local_experts,
                "recv_payload slot dimension is invalid");
    int hidden = fused_hidden - 2;
    int max_messages_per_rank = fused_slots_per_rank - num_local_experts;
    int num_recv_slots = num_ranks * num_max_dispatch_tokens_per_rank;
    auto int32_opts = torch::TensorOptions()
                          .dtype(torch::kInt32)
                          .device(recv_payload.device());
    auto int64_opts = torch::TensorOptions()
                          .dtype(torch::kInt64)
                          .device(recv_payload.device());

    auto packed_recv_x = torch::zeros(
        {num_local_experts, num_recv_slots, hidden}, recv_payload.options());
    auto packed_recv_src_info =
        torch::full({num_local_experts, num_recv_slots}, -1, int32_opts);
    auto layout_range =
        torch::zeros({num_local_experts, num_ranks}, int64_opts);
    auto recv_count = torch::zeros({num_local_experts}, int32_opts);
    auto return_src_pos =
        torch::full({num_local_experts, num_recv_slots, 2}, -1, int64_opts);
    compact_dispatch_fused_kernel<<<num_ranks * num_local_experts *
                                        max_messages_per_rank,
                                    256, 0, stream>>>(
        reinterpret_cast<const nv_bfloat16*>(recv_payload.data_ptr()),
        layout_range.data_ptr<int64_t>(), recv_count.data_ptr<int32_t>(),
        return_src_pos.data_ptr<int64_t>(),
        reinterpret_cast<nv_bfloat16*>(packed_recv_x.data_ptr()),
        packed_recv_src_info.data_ptr<int32_t>(), hidden, num_ranks,
        num_local_experts, max_messages_per_rank, num_recv_slots);
    return {packed_recv_x, packed_recv_src_info, layout_range, recv_count,
            return_src_pos};
}

torch::Tensor torch_alltoall_pack_combine(const torch::Tensor& expert_buffers,
                                          const torch::Tensor& return_src_pos,
                                          int num_ranks,
                                          int max_messages_per_rank) {
    check_cuda(expert_buffers, "expert_buffers");
    check_cuda(return_src_pos, "return_src_pos");
    TORCH_CHECK(expert_buffers.scalar_type() == torch::kBFloat16,
                "expert_buffers must be bfloat16");
    TORCH_CHECK(return_src_pos.scalar_type() == torch::kInt64,
                "return_src_pos must be int64");
    TORCH_CHECK(expert_buffers.dim() == 3,
                "expert_buffers must be a 3D tensor");
    TORCH_CHECK(return_src_pos.dim() == 3 && return_src_pos.size(2) == 2,
                "return_src_pos must have shape [experts, slots, 2]");
    TORCH_CHECK(num_ranks > 0 && max_messages_per_rank > 0,
                "num_ranks and max_messages_per_rank must be positive");
    const c10::cuda::CUDAGuard guard(expert_buffers.device());
    auto stream = at::cuda::getCurrentCUDAStream();
    int hidden = static_cast<int>(expert_buffers.size(2));
    int num_recv_slots = static_cast<int>(expert_buffers.size(1));
    auto send_payload = torch::zeros({num_ranks, max_messages_per_rank, hidden},
                                     expert_buffers.options());
    int num_local_experts = static_cast<int>(expert_buffers.size(0));
    pack_combine_kernel<<<num_local_experts * num_recv_slots, 256, 0, stream>>>(
        reinterpret_cast<const nv_bfloat16*>(expert_buffers.data_ptr()),
        return_src_pos.data_ptr<int64_t>(),
        reinterpret_cast<nv_bfloat16*>(send_payload.data_ptr()), hidden,
        max_messages_per_rank, num_recv_slots);
    return send_payload;
}

torch::Tensor torch_alltoall_reduce_combine(
    const torch::Tensor& recv_payload, const torch::Tensor& send_route,
    const torch::Tensor& topk_weights,
    const std::optional<torch::Tensor>& out) {
    check_cuda(recv_payload, "recv_payload");
    check_cuda(send_route, "send_route");
    check_cuda(topk_weights, "topk_weights");
    TORCH_CHECK(recv_payload.scalar_type() == torch::kBFloat16,
                "recv_payload must be bfloat16");
    TORCH_CHECK(send_route.scalar_type() == torch::kInt64,
                "send_route must be int64");
    TORCH_CHECK(topk_weights.scalar_type() == torch::kFloat32,
                "topk_weights must be float32");
    TORCH_CHECK(recv_payload.dim() == 3 && send_route.dim() == 2 &&
                    topk_weights.dim() == 2,
                "recv_payload, send_route, and topk_weights must be 3D/2D/2D");
    TORCH_CHECK(send_route.size(1) == 4,
                "send_route must have shape [tokens * topk, 4]");
    const c10::cuda::CUDAGuard guard(recv_payload.device());
    auto stream = at::cuda::getCurrentCUDAStream();
    int hidden = static_cast<int>(recv_payload.size(2));
    int max_messages_per_rank = static_cast<int>(recv_payload.size(1));
    int num_tokens = static_cast<int>(topk_weights.size(0));
    int num_topk = static_cast<int>(topk_weights.size(1));
    TORCH_CHECK(send_route.size(0) == num_tokens * num_topk,
                "send_route length must match topk_weights");
    auto combined = out.has_value() ? out.value()
                                    : torch::empty({num_tokens, hidden},
                                                   recv_payload.options());
    TORCH_CHECK(combined.scalar_type() == torch::kBFloat16,
                "out must be bfloat16");
    dim3 grid(num_tokens, ceil_div(hidden, 256));
    reduce_combine_kernel<<<grid, 256, 0, stream>>>(
        reinterpret_cast<const nv_bfloat16*>(recv_payload.data_ptr()),
        send_route.data_ptr<int64_t>(), topk_weights.data_ptr<float>(),
        reinterpret_cast<nv_bfloat16*>(combined.data_ptr()), num_tokens, hidden,
        num_topk, max_messages_per_rank);
    return combined;
}

}  // namespace mooncake
