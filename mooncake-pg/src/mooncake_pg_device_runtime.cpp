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

}  // namespace

void MooncakeBackend::ensureCudaDevice() const {
    if (isCpu_ || device_collective_runtime_.cuda_device_index < 0) return;
    cudaError_t err = cudaSetDevice(device_collective_runtime_.cuda_device_index);
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
    TORCH_CHECK(static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
                "Device API collective runtime active size exceeds rank limit.");

    auto& runtime = device_collective_runtime_;
    const bool debug = deviceCollectiveRuntimeDebugEnabled();
    const size_t staging_bytes = kBufferSize;
    auto debug_log = [&](const std::string& stage) {
        if (!debug) return;
        LOG(INFO) << "MOONCAKE_PG_DEVICE_RUNTIME debug rank=" << rank_
                  << " backend_index=" << meta_->backendIndex
                  << " stage=" << stage << " size=" << meta_->activeSize;
    };

    ensureCudaDevice();

    cudaError_t memset_err = cudaMemset(recv_buffer_[0], 0, staging_bytes);
    TORCH_CHECK(memset_err == cudaSuccess,
                "Failed to clear Device API collective staging buffer: ",
                cudaGetErrorString(memset_err));

    runtime.p2p_transport = device::createP2pDeviceTransport(meta_->activeSize);
    TORCH_CHECK(runtime.p2p_transport,
                "Failed to create Device API collective P2P transport.");
    auto local_handle = runtime.p2p_transport->exportIpcHandle(recv_buffer_[0]);
    std::vector<uint8_t> local_bytes(local_handle.size() * sizeof(int32_t));
    if (!local_handle.empty()) {
        std::memcpy(local_bytes.data(), local_handle.data(), local_bytes.size());
    }
    meta_->store->set(deviceCollectiveP2pKey(meta_->backendIndex, rank_),
                      local_bytes);
    debug_log("p2p_metadata_published");

    std::vector<std::string> p2p_keys;
    p2p_keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        p2p_keys.push_back(deviceCollectiveP2pKey(meta_->backendIndex, j));
    }
    BackoffWaiter p2p_waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
    p2p_waiter.wait([&] { return meta_->store->check(p2p_keys); });
    debug_log("p2p_metadata_wait_done");

    std::vector<std::vector<int32_t>> remote_handles(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        auto data = meta_->store->get(p2p_keys[j]);
        TORCH_CHECK(data.size() % sizeof(int32_t) == 0,
                    "Invalid Device API collective IPC handle size.");
        remote_handles[j].resize(data.size() / sizeof(int32_t));
        if (!data.empty()) {
            std::memcpy(remote_handles[j].data(), data.data(), data.size());
        }
    }

    std::vector<int> active_mask(meta_->activeSize, 1);
    runtime.p2p_transport->importPeerHandles(
        recv_buffer_[0], rank_, meta_->activeSize, remote_handles, active_mask);
    runtime.direct_p2p_ready = runtime.p2p_transport->allPeersAccessible();
    runtime.p2p_peer_ptrs_host.assign(meta_->activeSize, nullptr);
    cudaError_t peer_copy_err = cudaMemcpy(
        runtime.p2p_peer_ptrs_host.data(),
        runtime.p2p_transport->peerPtrsTablePtr(),
        runtime.p2p_peer_ptrs_host.size() * sizeof(void*),
        cudaMemcpyDeviceToHost);
    TORCH_CHECK(peer_copy_err == cudaSuccess,
                "Failed to copy Device API collective peer pointer table: ",
                cudaGetErrorString(peer_copy_err));
    debug_log("p2p_import_done");

    std::vector<std::string> rdma_filters = deviceFilters_;
    runtime.rdma_qps_per_rank = 1;
    const int num_qps = meta_->activeSize * runtime.rdma_qps_per_rank;
    runtime.rdma_transport = device::createIbgdaDeviceTransport(rdma_filters);
    TORCH_CHECK(runtime.rdma_transport,
                "Failed to create Device API collective RDMA transport.");

    int rc = runtime.rdma_transport->initialize("", meta_->activeSize, num_qps);
    if (rc == 0) {
        rc = runtime.rdma_transport->registerMemory(recv_buffer_[0], staging_bytes);
    }
    if (rc == 0) {
        rc = runtime.rdma_transport->allocateControlBuffer();
    }
    if (rc == 0) {
        auto stream = at::cuda::getCurrentCUDAStream();
        rc = runtime.rdma_transport->createQueuePairs(stream.stream());
    }
    if (rc == 0) {
        meta_->store->set(
            deviceCollectiveRdmaKey(meta_->backendIndex, rank_),
            serializeRdmaMetadata(runtime.rdma_transport->localMetadata()));

        std::vector<std::string> rdma_keys;
        rdma_keys.reserve(meta_->activeSize);
        for (int j = 0; j < meta_->activeSize; ++j) {
            rdma_keys.push_back(deviceCollectiveRdmaKey(meta_->backendIndex, j));
        }
        BackoffWaiter rdma_waiter(
            BackoffWaiterConfig::constantSleep(std::chrono::milliseconds(10)));
        rdma_waiter.wait([&] { return meta_->store->check(rdma_keys); });

        std::vector<device::RdmaLocalMetadata> remote_meta(meta_->activeSize);
        for (int j = 0; j < meta_->activeSize; ++j) {
            remote_meta[j] =
                deserializeRdmaMetadata(meta_->store->get(rdma_keys[j]));
            TORCH_CHECK(remote_meta[j].qpns.size() >=
                            static_cast<size_t>(num_qps),
                        "Device API collective RDMA metadata missing QPNs.");
            TORCH_CHECK(remote_meta[j].lids.size() >=
                            static_cast<size_t>(num_qps),
                        "Device API collective RDMA metadata missing LIDs.");
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
            const int channel = i % runtime.rdma_qps_per_rank;
            const int peer_local_qp = rank_ * runtime.rdma_qps_per_rank + channel;
            remote_qpns[i] = remote_meta[peer_rank].qpns[peer_local_qp];
            remote_lids[i] = remote_meta[peer_rank].lids[peer_local_qp];
        }
        std::vector<int> rdma_active_mask(meta_->activeSize, 1);
        rc = runtime.rdma_transport->connectPeers(
            rank_, runtime.rdma_transport->isRoce(), remote_addrs, remote_keys,
            remote_qpns, remote_lids, subnet_prefixes, interface_ids,
            rdma_active_mask);
    }

    runtime.rdma_ready = (rc == 0);
    TORCH_CHECK(runtime.rdma_ready,
                "Device API collective RDMA runtime initialization failed, rc=",
                rc);

    const uint32_t initial_sequence = 1;
    cudaError_t seq_counter_err =
        cudaMalloc(&runtime.device_sequence_counter, sizeof(uint32_t));
    TORCH_CHECK(seq_counter_err == cudaSuccess,
                "Failed to allocate Device API collective sequence counter: ",
                cudaGetErrorString(seq_counter_err));
    cudaError_t seq_counter_copy_err = cudaMemcpy(
        runtime.device_sequence_counter, &initial_sequence,
        sizeof(uint32_t), cudaMemcpyHostToDevice);
    TORCH_CHECK(seq_counter_copy_err == cudaSuccess,
                "Failed to initialize Device API collective sequence counter: ",
                cudaGetErrorString(seq_counter_copy_err));
    const uint32_t slot_capacity = kDefaultDeviceSequenceSlotCapacity;
    cudaError_t seq_slots_err = cudaMalloc(
        &runtime.device_sequence_slots,
        static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
    TORCH_CHECK(seq_slots_err == cudaSuccess,
                "Failed to allocate Device API collective sequence slots: ",
                cudaGetErrorString(seq_slots_err));
    cudaError_t seq_slots_memset_err = cudaMemset(
        runtime.device_sequence_slots, 0,
        static_cast<size_t>(slot_capacity) * sizeof(uint32_t));
    TORCH_CHECK(seq_slots_memset_err == cudaSuccess,
                "Failed to clear Device API collective sequence slots: ",
                cudaGetErrorString(seq_slots_memset_err));
    runtime.device_sequence_slot_cursor = 0;
    debug_log("ready");
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
