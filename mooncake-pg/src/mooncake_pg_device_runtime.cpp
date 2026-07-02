#include <ATen/cuda/CUDAContext.h>
#include <cuda_alike.h>
#include <fcntl.h>
#include <mooncake_backend.h>
#include <sys/mman.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <sstream>
#include <vector>

#include "connection_poller.h"
#include "mooncake_pg_device_bootstrap.h"
#include "mooncake_pg_experimental.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

std::string deviceCollectiveShmNameKey(int backendIndex) {
    return "mooncake_pg_device_collective_shm_name/" +
           std::to_string(backendIndex);
}

std::string deviceCollectiveShmReadyKey(int backendIndex) {
    return "mooncake_pg_device_collective_shm_ready/" +
           std::to_string(backendIndex);
}

}  // namespace

void MooncakeBackend::ensureCudaDevice() const {
    if (isCpu_ || device_collective_runtime_.cuda_device_index < 0) return;
    cudaError_t err = cudaSetDevice(device_collective_runtime_.cuda_device_index);
    TORCH_CHECK(err == cudaSuccess,
                "Failed to restore Mooncake PG CUDA device ",
                device_collective_runtime_.cuda_device_index, ": ",
                cudaGetErrorString(err));
}

bool MooncakeBackend::allActivePeersOnSameHost() const {
    if (!meta_ || !meta_->store) return false;

    const std::string local_server_host = serverHostOnly(localServerName_);
    std::vector<std::string> server_name_keys;
    server_name_keys.reserve(meta_->activeSize);
    for (int j = 0; j < meta_->activeSize; ++j) {
        server_name_keys.push_back(
            ConnectionContext::getServerNameStoreKey(meta_->backendIndex, j));
    }
    BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
        std::chrono::milliseconds(10)));
    waiter.wait([&] { return meta_->store->check(server_name_keys); });

    for (int j = 0; j < meta_->activeSize; ++j) {
        if (!meta_->activeRanks[j]) continue;
        const auto peer_server_name = meta_->store->get(server_name_keys[j]);
        const std::string peer_server_name_str(peer_server_name.begin(),
                                               peer_server_name.end());
        if (serverHostOnly(peer_server_name_str) != local_server_host) {
            return false;
        }
    }
    return true;
}

void MooncakeBackend::maybeInitDeviceCollectiveRuntime() {
#ifdef MOONCAKE_EP_USE_MUSA
    return;
#else
    if (isCpu_ || !useDeviceApiCollectivesPoc()) return;
    TORCH_CHECK(meta_->store,
                "Device API collective runtime requires a valid Store.");
    TORCH_CHECK(static_cast<size_t>(meta_->activeSize) <= kMaxNumRanks,
                "Device API collective runtime active size exceeds rank limit.");

    auto& runtime = device_collective_runtime_;
    const bool debug = deviceCollectiveRuntimeDebugEnabled();
    const size_t staging_bytes = directP2pEffectiveBufferSize();
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

    const bool should_skip_single_host_rdma =
        runtime.direct_p2p_ready && allActivePeersOnSameHost();
    if (!should_skip_single_host_rdma) {
        std::vector<std::string> rdma_filters = deviceFilters_;
        runtime.rdma_qps_per_rank = 1;
        const int num_qps = meta_->activeSize * runtime.rdma_qps_per_rank;
        runtime.rdma_transport =
            device::createIbgdaDeviceTransport(rdma_filters);
        if (runtime.rdma_transport) {
            int rc =
                runtime.rdma_transport->initialize("", meta_->activeSize, num_qps);
            if (rc == 0) {
                rc = runtime.rdma_transport->registerMemory(recv_buffer_[0],
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
                meta_->store->set(
                    deviceCollectiveRdmaKey(meta_->backendIndex, rank_),
                    serializeRdmaMetadata(runtime.rdma_transport->localMetadata()));

                std::vector<std::string> rdma_keys;
                rdma_keys.reserve(meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    rdma_keys.push_back(
                        deviceCollectiveRdmaKey(meta_->backendIndex, j));
                }
                BackoffWaiter rdma_waiter(BackoffWaiterConfig::constantSleep(
                    std::chrono::milliseconds(10)));
                rdma_waiter.wait([&] { return meta_->store->check(rdma_keys); });

                std::vector<device::RdmaLocalMetadata> remote_meta(
                    meta_->activeSize);
                for (int j = 0; j < meta_->activeSize; ++j) {
                    remote_meta[j] =
                        deserializeRdmaMetadata(meta_->store->get(rdma_keys[j]));
                    TORCH_CHECK(remote_meta[j].qpns.size() >=
                                    static_cast<size_t>(num_qps),
                                "Device API collective RDMA metadata missing "
                                "QPNs.");
                    TORCH_CHECK(remote_meta[j].lids.size() >=
                                    static_cast<size_t>(num_qps),
                                "Device API collective RDMA metadata missing "
                                "LIDs.");
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
                    const int peer_local_qp =
                        rank_ * runtime.rdma_qps_per_rank + channel;
                    remote_qpns[i] = remote_meta[peer_rank].qpns[peer_local_qp];
                    remote_lids[i] = remote_meta[peer_rank].lids[peer_local_qp];
                }
                std::vector<int> rdma_active_mask(meta_->activeSize, 1);
                rc = runtime.rdma_transport->connectPeers(
                    rank_, runtime.rdma_transport->isRoce(), remote_addrs,
                    remote_keys, remote_qpns, remote_lids, subnet_prefixes,
                    interface_ids, rdma_active_mask);
            }

            runtime.rdma_ready = (rc == 0);
            if (!runtime.rdma_ready) {
                LOG(WARNING)
                    << "MOONCAKE_PG_DEVICE_RUNTIME rdma init failed rank="
                    << rank_ << " rc=" << rc;
                runtime.rdma_transport.reset();
            }
        }
    }

    if (!runtime.rdma_ready) {
        const size_t signal_bytes =
            static_cast<size_t>(meta_->activeSize) * 2 * sizeof(uint32_t);
        std::string shm_name;
        const std::string shm_name_key =
            deviceCollectiveShmNameKey(meta_->backendIndex);
        const std::string shm_ready_key =
            deviceCollectiveShmReadyKey(meta_->backendIndex);
        if (rank_ == 0) {
            shm_name = "/mooncake_pg_device_runtime_" +
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
            TORCH_CHECK(shm_fd >= 0,
                        "Failed to create Device API collective shm region.");
            TORCH_CHECK(ftruncate(shm_fd, signal_bytes) == 0,
                        "Failed to size Device API collective shm region.");
        } else {
            BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                std::chrono::milliseconds(10)));
            waiter.wait([&] {
                shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0600);
                return shm_fd >= 0;
            });
        }
        void* mapped = mmap(nullptr, signal_bytes, PROT_READ | PROT_WRITE,
                            MAP_SHARED, shm_fd, 0);
        TORCH_CHECK(mapped != MAP_FAILED,
                    "Failed to mmap Device API collective shm signal region.");
        close(shm_fd);
        runtime.host_signals = static_cast<uint32_t*>(mapped);
        runtime.host_signals_bytes = signal_bytes;
        if (rank_ == 0) {
            std::memset(runtime.host_signals, 0, signal_bytes);
            meta_->store->set(shm_ready_key, "1");
        } else {
            BackoffWaiter waiter(BackoffWaiterConfig::constantSleep(
                std::chrono::milliseconds(10)));
            waiter.wait([&] { return meta_->store->check({shm_ready_key}); });
        }
    }

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
    const uint32_t slot_capacity = directP2pDeviceSequenceSlotCapacity();
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
    runtime.direct_p2p_sequence = initial_sequence;
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
    runtime.direct_p2p_sequence = 1;
    runtime.p2p_transport.reset();
    runtime.rdma_transport.reset();
    runtime.direct_p2p_ready = false;
    runtime.rdma_ready = false;
    runtime.p2p_peer_ptrs_host.clear();
    if (runtime.host_signals && runtime.host_signals_bytes > 0) {
        munmap(runtime.host_signals, runtime.host_signals_bytes);
    }
    runtime.host_signals = nullptr;
    runtime.host_signals_bytes = 0;
}

}  // namespace mooncake
