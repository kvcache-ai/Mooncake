#include "device_comm/device_collective/algorithms/tree/tree_types.cuh"

#include <new>
#include <type_traits>

#include <cooperative_groups.h>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"

namespace mooncake {
namespace {

constexpr int kBlockThreads = 512;

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runTreeChannel(const AllReduceRequest& request, TreeAllReduceDeviceState* state,
               uint32_t channel, cooperative_groups::thread_block block) {
    const auto plan = state->plan;
    if (request.count == 0) return {};
    const auto channels =
        chooseTreeChannelCount(request.count, sizeof(T), plan.workspace.bytes,
                               kMaxDeviceCollectiveChannels);
    PG_ASSERT(channels != 0 && plan.tree_count == 1);
    if (channel >= channels) return {};
    const auto binding =
        TreeChannelBinding::make(request.count, channel, channels);
    const auto* input = static_cast<const T*>(request.send_buffer);
    auto* output = static_cast<T*>(request.recv_buffer);
    if (plan.participant_count == 1) {
        if (input != output)
            copyValuesTo(input + binding.element_offset, binding.element_count,
                         block, output + binding.element_offset);
        return {};
    }
    const auto buffers = TreeBufferLayout::make(plan.workspace.bytes, channels);
    const auto tile_elements = buffers.chunk_bytes / sizeof(T);
    const auto& topology = plan.topologies[binding.tree_id];
    using Primitive = SimplePrimitives<T, Op>;
    static_assert(std::is_trivially_destructible_v<Primitive>);
    // Exactly one object/cursor snapshot per (channel, neighbor), used in both
    // phases. Logical role indices preserve absent child slots.
    alignas(Primitive) unsigned char storage[kTreeReceiveRoleCount]
                                            [sizeof(Primitive)];
    Primitive* connections[kTreeReceiveRoleCount] = {};
    const CollectivePeer peers[kTreeReceiveRoleCount] = {
        topology.children[0], topology.children[1], topology.parent};
    for (uint32_t role = 0; role < kTreeReceiveRoleCount; ++role) {
        if (peers[role].global_rank == kInvalidGlobalRank) continue;
        const auto local_role = static_cast<TreeReceiveRole>(role);
        const auto remote_role =
            role == static_cast<uint32_t>(TreeReceiveRole::Parent)
                ? static_cast<TreeReceiveRole>(topology.parent_child_slot)
                : TreeReceiveRole::Parent;
        const auto local = buffers.connection(channel, local_role);
        const auto remote = buffers.connection(channel, remote_role);
        connections[role] = ::new (static_cast<void*>(storage[role])) Primitive(
            Primitive::bindConnection(*state->simple, plan.workspace,
                                      peers[role], peers[role], channel,
                                      {{buffers.chunk_bytes},
                                       local.offset,
                                       remote.offset,
                                       local.offset},
                                      state->collective.timeout_ticks));
    }
    for (auto* connection : connections)
        if (connection) connection->publishRecvBufferReady(block);
    for (auto* connection : connections) {
        if (!connection) continue;
        const auto result = connection->waitSendBufferReady(block);
        if (!result.succeeded()) return result;
    }

    auto* parent = connections[static_cast<uint32_t>(TreeReceiveRole::Parent)];
    const auto tile_count = binding.tileCount(tile_elements);
    // Complete the upward stream before starting the downward stream. With
    // bounded two-slot connections this avoids a cross-phase wait cycle.
    for (uint64_t index = 0; index < tile_count; ++index) {
        const auto tile = binding.tile(index, tile_elements);
        const T* partial = input + tile.element_offset;
        auto* destination = output + tile.element_offset;
        for (uint32_t child = 0; child < kTreeChildCount; ++child) {
            if (!connections[child]) continue;
            const auto result = connections[child]->recvReduceCopy(
                {partial, destination, tile.element_count}, block);
            if (!result.succeeded()) return result;
            partial = destination;
        }
        if (parent) {
            const auto result =
                parent->send({partial, nullptr, tile.element_count}, block);
            if (!result.succeeded()) return result;
        } else if (partial != destination) {
            copyValuesTo(partial, tile.element_count, block, destination);
        }
    }
    for (uint64_t index = 0; index < tile_count; ++index) {
        const auto tile = binding.tile(index, tile_elements);
        auto* destination = output + tile.element_offset;
        if (parent) {
            const auto result = parent->recvCopy(
                {nullptr, destination, tile.element_count}, block);
            if (!result.succeeded()) return result;
        }
        for (uint32_t child = 0; child < kTreeChildCount; ++child) {
            if (!connections[child]) continue;
            const auto result = connections[child]->send(
                {destination, nullptr, tile.element_count}, block);
            if (!result.succeeded()) return result;
        }
    }
    for (auto* connection : connections) {
        if (!connection) continue;
        const auto result = connection->finish(block);
        if (!result.succeeded()) return result;
    }
    return {};
}

template <typename T, ReduceOp Op>
__global__ __launch_bounds__(kBlockThreads, 1) void treeAllReduceKernel(
    AllReduceRequest request, TreeAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    const uint32_t channel = blockIdx.x;
    PG_ASSERT(state && state->simple);
    auto result = beginCollectiveInvocation<CollectiveExecution::MultiCTA>(
        &state->plan, state->collective, block, [&] {
            return detail::synchronizeCollectiveViewEpoch(
                state->plan.view_epoch, state->collective.timeout_ticks,
                state->collective.view_epoch_signals, state->plan.remotePeers(),
                state->collective.transfer_handle->lane(channel), block);
        });
    if (result.succeeded())
        result = runTreeChannel<T, Op>(request, state, channel, block);
    finishCollectiveInvocation<CollectiveExecution::MultiCTA>(
        state->collective, state->plan.remotePeers(), block, result.failed_rank,
        request.failed_ranks_hint);
}

template <typename T, ReduceOp Op>
cudaError_t launchKernel(const AllReduceRequest& request,
                         TreeAllReduceDeviceState* state, cudaStream_t stream) {
    treeAllReduceKernel<T, Op>
        <<<kMaxDeviceCollectiveChannels, kBlockThreads, 0, stream>>>(request,
                                                                     state);
    return cudaGetLastError();
}

template <typename T>
cudaError_t launchForType(const AllReduceRequest& request,
                          TreeAllReduceDeviceState* state,
                          cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            return launchKernel<T, ReduceOp::Sum>(request, state, stream);
        case ReduceOp::Product:
            return launchKernel<T, ReduceOp::Product>(request, state, stream);
        case ReduceOp::Min:
            return launchKernel<T, ReduceOp::Min>(request, state, stream);
        case ReduceOp::Max:
            return launchKernel<T, ReduceOp::Max>(request, state, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace

cudaError_t launchTreeAllReduceKernel(const AllReduceRequest& request,
                                      TreeAllReduceDeviceState* state,
                                      cudaStream_t stream) {
    switch (request.datatype) {
        case DataType::Float16:
            return launchForType<__half>(request, state, stream);
        case DataType::Uint8:
            return launchForType<uint8_t>(request, state, stream);
        case DataType::Int8:
            return launchForType<int8_t>(request, state, stream);
        case DataType::Int16:
            return launchForType<int16_t>(request, state, stream);
        case DataType::Int32:
            return launchForType<int32_t>(request, state, stream);
        case DataType::Int64:
            return launchForType<int64_t>(request, state, stream);
        case DataType::Bfloat16:
            return launchForType<__nv_bfloat16>(request, state, stream);
        case DataType::Float32:
            return launchForType<float>(request, state, stream);
        case DataType::Float64:
            return launchForType<double>(request, state, stream);
        case DataType::Bool:
            return launchForType<bool>(request, state, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
