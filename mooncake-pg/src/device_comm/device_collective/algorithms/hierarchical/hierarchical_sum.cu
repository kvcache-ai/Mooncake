#include "device_comm/device_collective/algorithms/hierarchical/hierarchical_kernel.cuh"

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ll/ll_primitives.cuh"
#include "device_comm/device_collective/protocols/ll/ll_value_pack.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"

namespace mooncake {
namespace {

using Pack = LLValuePack<float>;

__device__ void sendLocal(const HierarchicalKernelPlan& plan,
                          const DeviceGroupView& view, const LLState& ll,
                          uint32_t peer, const float* values, uint64_t count,
                          uint64_t sequence, float scale, uint64_t timeout,
                          cooperative_groups::thread_block block) {
    auto* packets = static_cast<LLPacket*>(ll.transfer_handle->remotePtr(
        view.participants[peer].global_rank, plan.packet_offsets[peer]));
    PG_ASSERT(packets);
    const auto slot =
        plan.layout.packetIndex(0, sequence % 2, view.self().in_group_rank);
    LLPacketOps ops(sequence, timeout);
    for (uint64_t i = block.thread_rank(); i < count; i += block.size())
        ops.storePack<Pack>(packets + slot + i,
                            __float_as_uint(values[i] * scale));
    block.sync();
}

__device__ InGroupRank readLocal(const HierarchicalKernelPlan& plan,
                                 const DeviceGroupView& view, uint32_t peer,
                                 float* output, const float* input,
                                 uint64_t count, uint64_t sequence, float scale,
                                 uint64_t timeout,
                                 cooperative_groups::thread_block block) {
    const auto rank = view.participants[peer].in_group_rank;
    const auto slot = plan.layout.packetIndex(0, sequence % 2, rank);
    LLPacketOps ops(sequence, timeout);
    InGroupRank failed = kInvalidInGroupRank;
    for (uint64_t i = block.thread_rank(); i < count; i += block.size()) {
        uint32_t bits;
        if (!ops.loadPack<Pack>(plan.packets + slot + i, bits)) {
            failed = rank;
            break;
        }
        const float received = __uint_as_float(bits);
        output[i] = input ? input[i] * scale + received : received;
    }
    block.sync();
    return failed;
}

__device__ CollectiveResult runChunk(const HierarchicalKernelState& state,
                                     const float* input, float* output,
                                     uint64_t count, float scale,
                                     cooperative_groups::thread_block block) {
    const auto& plan = state.plan;
    const auto& view = *state.collective.group_view;
    auto& ll = *state.ll;
    const auto timeout = state.collective.timeout_ticks;
    LLControl control(ll, view, 0, timeout);
    const bool leads = view.self_active_index == plan.local_leader;
    const auto packet_bytes = plan.layout.channelPacketBytes();

    PG_COLL_TRY(control.begin(plan.packets, packet_bytes, block));
    auto sequence = ll.progress[0].next_sequence;
    InGroupRank failed = kInvalidInGroupRank;
    if (leads) {
        for (uint64_t i = block.thread_rank(); i < count; i += block.size())
            output[i] = input[i] * scale;
        block.sync();
        for (uint32_t slot = 0; slot < ll.peer_count; ++slot) {
            const auto peer = ll.peer_active_indices[slot];
            const auto result =
                readLocal(plan, view, peer, output, output, count, sequence,
                          1.0f, timeout, block);
            if (result != kInvalidInGroupRank) failed = result;
        }
    } else {
        sendLocal(plan, view, ll, plan.local_leader, input, count, sequence,
                  scale, timeout, block);
    }
    PG_COLL_TRY(control.finish(sequence, failed, block));

    if (leads) {
        // The two-node prototype has one neighbor in both directions.
        auto ring = SimplePrimitives<float>::bind(
            *state.simple, plan.workspace, view, plan.remote_leader,
            plan.remote_leader, 0, 1, OneShotBufferLayout::kChunkBytes,
            timeout);
        PG_COLL_TRY(ring.begin(block));
        const CollectiveChunk<float> partial{output, output, count};
        PG_COLL_TRY(ring.send(partial, block));
        PG_COLL_TRY(ring.recvTransformCopy(partial, block,
                                           Reduce<float, ReduceOp::Sum>{}));
        PG_COLL_TRY(ring.finish(block));
    }

    PG_COLL_TRY(control.begin(plan.packets, packet_bytes, block));
    sequence = ll.progress[0].next_sequence;
    if (leads) {
        for (uint32_t slot = 0; slot < ll.peer_count; ++slot) {
            sendLocal(plan, view, ll, ll.peer_active_indices[slot], output,
                      count, sequence, 1.0f, timeout, block);
        }
    } else {
        failed = readLocal(plan, view, plan.local_leader, output, nullptr,
                           count, sequence, 1.0f, timeout, block);
    }
    return control.finish(sequence, failed, block);
}

__global__ void hierarchicalSumKernel(AllReduceRequest request,
                                      HierarchicalKernelState* state) {
    runCollectiveInvocation<CollectiveExecution::SingleCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) -> CollectiveResult {
            const auto& view = *state->collective.group_view;
            const float scale = request.op == ReduceOp::Avg
                                    ? reciprocal<float>(view.participant_count)
                                    : 1.0f;
            constexpr uint64_t chunk_elements =
                OneShotBufferLayout::kChunkBytes / sizeof(float);
            for (uint64_t offset = 0; offset < request.count;
                 offset += chunk_elements) {
                const auto count =
                    minimum(chunk_elements, request.count - offset);
                PG_COLL_TRY(runChunk(
                    *state,
                    static_cast<const float*>(request.send_buffer) + offset,
                    static_cast<float*>(request.recv_buffer) + offset, count,
                    scale, block));
            }
            return {};
        });
}

}  // namespace

PGResult<void> launchHierarchicalSumKernel(const AllReduceRequest& request,
                                           HierarchicalKernelState* state,
                                           cudaStream_t stream) {
    PG_VALIDATE_ARG(
        request.datatype == DataType::Float32 &&
            (request.op == ReduceOp::Sum || request.op == ReduceOp::Avg),
        "Hierarchical prototype requires float32 Sum or Avg");
    hierarchicalSumKernel<<<1, 128, 0, stream>>>(request, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
