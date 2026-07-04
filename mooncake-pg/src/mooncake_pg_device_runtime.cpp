#include <ATen/cuda/CUDAContext.h>
#include <cuda_alike.h>
#include <mooncake_backend.h>

#include <chrono>
#include <cstring>
#include <cstdlib>
#include <vector>

#include "connection_poller.h"
#include "mooncake_pg_device_bootstrap.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

bool envEnabled(const char* name) {
    const char* value = std::getenv(name);
    return value && value[0] != '\0' && value[0] != '0';
}

bool deviceCollectivesEnabled() {
    return envEnabled("MOONCAKE_PG_DEVICE_API_COLLECTIVES");
}

bool deviceCollectiveRuntimeDebugEnabled() {
    return envEnabled("MOONCAKE_PG_DEVICE_RUNTIME_DEBUG");
}

constexpr uint32_t kDefaultDeviceSequenceSlotCapacity = 1u << 20;

void logDeviceCollectiveRuntimeStage(bool debug, int rank, int backend_index,
                                     int active_size,
                                     const std::string& stage) {
    if (!debug) return;
    LOG(INFO) << "MOONCAKE_PG_DEVICE_RUNTIME debug rank=" << rank
              << " backend_index=" << backend_index << " stage=" << stage
              << " size=" << active_size;
}

void initDeviceCollectiveP2pRuntime(
    DeviceCollectiveRuntimeState& runtime,
    const c10::intrusive_ptr<c10d::Store>& store, int backend_index, int rank,
    int active_size, void* staging_buffer, bool debug) {
    // Phase 2: publish/import same-host IPC handles so the runtime owns a
    // stable peer-pointer table before any collective launch.
    runtime.p2p_transport = device::createP2pDeviceTransport(active_size);
    TORCH_CHECK(runtime.p2p_transport,
                "Failed to create Device API collective P2P transport.");

    auto local_handle = runtime.p2p_transport->exportIpcHandle(staging_buffer);
    std::vector<uint8_t> local_bytes(local_handle.size() * sizeof(int32_t));
    if (!local_handle.empty()) {
        std::memcpy(local_bytes.data(), local_handle.data(),
                    local_bytes.size());
    }
    store->set(deviceCollectiveP2pKey(backend_index, rank), local_bytes);
    logDeviceCollectiveRuntimeStage(debug, rank, backend_index, active_size,
                                    "p2p_metadata_published");

    std::vector<std::string> p2p_keys;
    p2p_keys.reserve(active_size);
    for (int peer = 0; peer < active_size; ++peer) {
        p2p_keys.push_back(deviceCollectiveP2pKey(backend_index, peer));
    }
    BackoffWaiter p2p_waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
    p2p_waiter.wait([&] { return store->check(p2p_keys); });
    logDeviceCollectiveRuntimeStage(debug, rank, backend_index, active_size,
                                    "p2p_metadata_wait_done");

    std::vector<std::vector<int32_t>> remote_handles(active_size);
    for (int peer = 0; peer < active_size; ++peer) {
        auto data = store->get(p2p_keys[peer]);
        TORCH_CHECK(data.size() % sizeof(int32_t) == 0,
                    "Invalid Device API collective IPC handle size.");
        remote_handles[peer].resize(data.size() / sizeof(int32_t));
        if (!data.empty()) {
            std::memcpy(remote_handles[peer].data(), data.data(), data.size());
        }
    }

    std::vector<int> active_mask(active_size, 1);
    runtime.p2p_transport->importPeerHandles(staging_buffer, rank, active_size,
                                             remote_handles, active_mask);
    runtime.direct_p2p_ready = runtime.p2p_transport->allPeersAccessible();
    runtime.p2p_peer_ptrs_host.assign(active_size, nullptr);
    cudaError_t peer_copy_err =
        cudaMemcpy(runtime.p2p_peer_ptrs_host.data(),
                   runtime.p2p_transport->peerPtrsTablePtr(),
                   runtime.p2p_peer_ptrs_host.size() * sizeof(void*),
                   cudaMemcpyDeviceToHost);
    TORCH_CHECK(peer_copy_err == cudaSuccess,
                "Failed to copy Device API collective peer pointer table: ",
                cudaGetErrorString(peer_copy_err));
    logDeviceCollectiveRuntimeStage(debug, rank, backend_index, active_size,
                                    "p2p_import_done");
}

void initDeviceCollectiveRdmaRuntime(
    DeviceCollectiveRuntimeState& runtime,
    const c10::intrusive_ptr<c10d::Store>& store, int backend_index, int rank,
    int active_size, void* staging_buffer, size_t staging_bytes,
    const std::vector<std::string>& device_filters, bool debug) {
    // Phase 3: publish/connect cross-node RDMA metadata in rank-stable order.
    runtime.rdma_qps_per_rank = 1;
    const int num_qps = active_size * runtime.rdma_qps_per_rank;
    runtime.rdma_transport = device::createIbgdaDeviceTransport(device_filters);
    TORCH_CHECK(runtime.rdma_transport,
                "Failed to create Device API collective RDMA transport.");

    int rc = runtime.rdma_transport->initialize("", active_size, num_qps);
    if (rc == 0) {
        rc = runtime.rdma_transport->registerMemory(staging_buffer,
                                                    staging_bytes);
    }
    if (rc == 0) {
        rc = runtime.rdma_transport->allocateControlBuffer();
    }
    if (rc == 0) {
        auto stream = at::cuda::getCurrentCUDAStream();
        rc = runtime.rdma_transport->createQueuePairs(stream.stream());
    }
    if (rc == 0) {
        store->set(
            deviceCollectiveRdmaKey(backend_index, rank),
            serializeRdmaMetadata(runtime.rdma_transport->localMetadata()));

        std::vector<std::string> rdma_keys;
        rdma_keys.reserve(active_size);
        for (int peer = 0; peer < active_size; ++peer) {
            rdma_keys.push_back(deviceCollectiveRdmaKey(backend_index, peer));
        }
        BackoffWaiter rdma_waiter(
            BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
        rdma_waiter.wait([&] { return store->check(rdma_keys); });

        std::vector<device::RdmaLocalMetadata> remote_meta(active_size);
        for (int peer = 0; peer < active_size; ++peer) {
            remote_meta[peer] =
                deserializeRdmaMetadata(store->get(rdma_keys[peer]));
            TORCH_CHECK(
                remote_meta[peer].qpns.size() >= static_cast<size_t>(num_qps),
                "Device API collective RDMA metadata missing QPNs.");
            TORCH_CHECK(
                remote_meta[peer].lids.size() >= static_cast<size_t>(num_qps),
                "Device API collective RDMA metadata missing LIDs.");
        }

        std::vector<int64_t> remote_addrs(active_size, 0);
        std::vector<int32_t> remote_keys(active_size, 0);
        std::vector<int64_t> subnet_prefixes(active_size, 0);
        std::vector<int64_t> interface_ids(active_size, 0);
        for (int peer = 0; peer < active_size; ++peer) {
            remote_addrs[peer] = remote_meta[peer].raddr;
            remote_keys[peer] = remote_meta[peer].rkey;
            subnet_prefixes[peer] = remote_meta[peer].subnet_prefix;
            interface_ids[peer] = remote_meta[peer].interface_id;
        }

        std::vector<int32_t> remote_qpns(num_qps, 0);
        std::vector<int32_t> remote_lids(num_qps, 0);
        for (int qp = 0; qp < num_qps; ++qp) {
            const int peer_rank = qp * active_size / num_qps;
            const int channel = qp % runtime.rdma_qps_per_rank;
            const int peer_local_qp =
                rank * runtime.rdma_qps_per_rank + channel;
            remote_qpns[qp] = remote_meta[peer_rank].qpns[peer_local_qp];
            remote_lids[qp] = remote_meta[peer_rank].lids[peer_local_qp];
        }
        std::vector<int> rdma_active_mask(active_size, 1);
        rc = runtime.rdma_transport->connectPeers(
            rank, runtime.rdma_transport->isRoce(), remote_addrs, remote_keys,
            remote_qpns, remote_lids, subnet_prefixes, interface_ids,
            rdma_active_mask);
    }

    runtime.rdma_ready = (rc == 0);
    TORCH_CHECK(
        runtime.rdma_ready,
        "Device API collective RDMA runtime initialization failed, rc=", rc);
    logDeviceCollectiveRuntimeStage(debug, rank, backend_index, active_size,
                                    "rdma_connect_done");
}

void initDeviceCollectiveSequenceState(DeviceCollectiveRuntimeState& runtime) {
    // Phase 4: allocate graph-safe device sequence state shared by later
    // collective launches.
    const uint32_t initial_sequence = 1;
    cudaError_t seq_counter_err =
        cudaMalloc(&runtime.device_sequence_counter, sizeof(uint32_t));
    TORCH_CHECK(seq_counter_err == cudaSuccess,
                "Failed to allocate Device API collective sequence counter: ",
                cudaGetErrorString(seq_counter_err));
    cudaError_t seq_counter_copy_err =
        cudaMemcpy(runtime.device_sequence_counter, &initial_sequence,
                   sizeof(uint32_t), cudaMemcpyHostToDevice);
    TORCH_CHECK(seq_counter_copy_err == cudaSuccess,
                "Failed to initialize Device API collective sequence counter: ",
                cudaGetErrorString(seq_counter_copy_err));
    const uint32_t slot_capacity = kDefaultDeviceSequenceSlotCapacity;
    cudaError_t seq_slots_err =
        cudaMalloc(&runtime.device_sequence_slots,
                   static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
    TORCH_CHECK(seq_slots_err == cudaSuccess,
                "Failed to allocate Device API collective sequence slots: ",
                cudaGetErrorString(seq_slots_err));
    cudaError_t seq_slots_memset_err =
        cudaMemset(runtime.device_sequence_slots, 0,
                   static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
    TORCH_CHECK(seq_slots_memset_err == cudaSuccess,
                "Failed to clear Device API collective sequence slots: ",
                cudaGetErrorString(seq_slots_memset_err));
    runtime.device_sequence_slot_cursor = 0;
}

}  // namespace

void MooncakeBackend::ensureCudaDevice() const {
    if (isCpu_ || device_collective_runtime_.cuda_device_index < 0) return;
    cudaError_t err =
        cudaSetDevice(device_collective_runtime_.cuda_device_index);
    TORCH_CHECK(err == cudaSuccess,
                "Failed to restore Mooncake PG CUDA device ",
                device_collective_runtime_.cuda_device_index, ": ",
                cudaGetErrorString(err));
}

void MooncakeBackend::maybeInitDeviceCollectiveRuntime() {
#ifdef MOONCAKE_EP_USE_MUSA
    return;
#else
    if (isCpu_ || !deviceCollectivesEnabled()) return;
    TORCH_CHECK(meta_->store,
                "Device API collective runtime requires a valid Store.");
    TORCH_CHECK(
        static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
        "Device API collective runtime active size exceeds rank limit.");

    auto& runtime = device_collective_runtime_;
    const bool debug = deviceCollectiveRuntimeDebugEnabled();
    const size_t staging_bytes = kBufferSize;

    ensureCudaDevice();

    // Phase 1: clear the backend-owned staging buffer that later collectives
    // will route through this runtime.
    cudaError_t memset_err = cudaMemset(recv_buffer_[0], 0, staging_bytes);
    TORCH_CHECK(memset_err == cudaSuccess,
                "Failed to clear Device API collective staging buffer: ",
                cudaGetErrorString(memset_err));
    initDeviceCollectiveP2pRuntime(runtime, meta_->store, meta_->backendIndex,
                                   rank_, meta_->activeSize, recv_buffer_[0],
                                   debug);
    initDeviceCollectiveRdmaRuntime(runtime, meta_->store, meta_->backendIndex,
                                    rank_, meta_->activeSize, recv_buffer_[0],
                                    staging_bytes, deviceFilters_, debug);
    initDeviceCollectiveSequenceState(runtime);
    logDeviceCollectiveRuntimeStage(debug, rank_, meta_->backendIndex,
                                    meta_->activeSize, "ready");
#endif
}

void MooncakeBackend::resetDeviceCollectiveRuntime() {
    auto& runtime = device_collective_runtime_;

    if (runtime.device_sequence_counter) {
        cudaFree(runtime.device_sequence_counter);
        runtime.device_sequence_counter = nullptr;
    }
    if (runtime.device_sequence_slots) {
        cudaFree(runtime.device_sequence_slots);
        runtime.device_sequence_slots = nullptr;
    }
    runtime.device_sequence_slot_cursor = 0;
    runtime.p2p_transport.reset();
    runtime.rdma_transport.reset();
    runtime.direct_p2p_ready = false;
    runtime.rdma_ready = false;
    runtime.p2p_peer_ptrs_host.clear();
}

}  // namespace mooncake
