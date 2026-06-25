#include <gflags/gflags.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "cuda_alike.h"
#include "transport/device/ibgda/mlx5gda.h"
#include "transport/device/device_ops.cuh"
#include "transport/device/device_transport.h"

DEFINE_string(device, "", "RDMA device name, e.g. mlx5_10. Empty means auto");
DEFINE_int32(gpu_id, 0, "CUDA-like device id");
DEFINE_bool(loopback, false, "Post a self RDMA WRITE from device kernel");

namespace {

__global__ void probeKernel(mlx5gda_qp_devctx* qps, uint64_t* out) {
    mlx5gda_qp_devctx* qp = qps;
    out[0] = reinterpret_cast<uint64_t>(qp->wq);
    out[1] = reinterpret_cast<uint64_t>(qp->dbr);
    out[2] = reinterpret_cast<uint64_t>(qp->bf);
    if (qp->wq == nullptr || qp->dbr == nullptr) {
        out[3] = 1;
        return;
    }

    constexpr uint64_t kWqPattern = 0x1122334455667788ULL;
    constexpr uint32_t kDbrPattern = 0x01020304U;
    qp->wq[0].qwords[0] = kWqPattern;
    qp->dbr->send_counter = kDbrPattern;
    out[3] = qp->wq[0].qwords[0];
    out[4] = qp->dbr->send_counter;
}

__device__ __forceinline__ void postDb(mlx5gda_qp_devctx* qp) {
    uint32_t num_posted = static_cast<uint32_t>(qp->wq_head);
    mooncake::device::mc_st_release_u32(
        reinterpret_cast<uint32_t*>(&qp->dbr->send_counter),
        mooncake::device::mc_bswap32(num_posted));
    if (qp->bf != nullptr) {
        auto* last_wqe = qp->wq + ((num_posted - 1) & qp->wqeid_mask);
        mooncake::device::mc_st_release_u64(
            reinterpret_cast<uint64_t*>(qp->bf + qp->bf_offset),
            *reinterpret_cast<uint64_t*>(last_wqe));
        qp->bf_offset ^= MLX5GDA_BF_SIZE;
    }
}

__global__ void loopbackKernel(mlx5gda_qp_devctx* qps, uint64_t src,
                               uint64_t dst, uint32_t rkey, uint64_t* out) {
    mlx5gda_qp_devctx* qp = qps;
    auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(
        qp->wq + (qp->wq_head & qp->wqeid_mask));

    wqe->ctrl = {};
    wqe->ctrl.qpn_ds = mooncake::device::mc_bswap32((qp->qpn << 8) | 3);
    wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    wqe->ctrl.opmod_idx_opcode = mooncake::device::mc_bswap32(
        (static_cast<uint32_t>(qp->wq_head) << 8) | MLX5_OPCODE_RDMA_WRITE);
    wqe->raddr.raddr = mooncake::device::mc_bswap64(dst);
    wqe->raddr.rkey = mooncake::device::mc_bswap32(rkey);
    wqe->raddr.reserved = 0;
    wqe->data.byte_count = mooncake::device::mc_bswap32(sizeof(uint64_t));
    wqe->data.lkey = mooncake::device::mc_bswap32(rkey);
    wqe->data.addr = mooncake::device::mc_bswap64(src);

    ++qp->wq_head;
    postDb(qp);

    out[0] = qp->qpn;
    out[1] = qp->wq_head;
    out[2] = reinterpret_cast<uint64_t>(qp->bf);
}

void checkCuda(cudaError_t err, const char* what) {
    if (err != cudaSuccess) {
        throw std::runtime_error(std::string(what) + ": " +
                                 cudaGetErrorString(err));
    }
}

void check(bool cond, const char* what) {
    if (!cond) throw std::runtime_error(what);
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try {
        int device_count = 0;
        checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
        check(FLAGS_gpu_id >= 0 && FLAGS_gpu_id < device_count,
              "invalid --gpu_id");
        checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice");

        auto rdma = mooncake::device::createIbgdaDeviceTransport();
        check(static_cast<bool>(rdma),
              "createIbgdaDeviceTransport returned null");
        check(rdma->initialize(FLAGS_device, 1, 1) == 0,
              "RdmaTransport::initialize failed");
        check(rdma->allocateControlBuffer() == 0,
              "RdmaTransport::allocateControlBuffer failed");

        cudaStream_t stream = nullptr;
        checkCuda(cudaStreamCreate(&stream), "cudaStreamCreate");
        check(rdma->createQueuePairs(reinterpret_cast<void*>(stream)) == 0,
              "RdmaTransport::createQueuePairs failed");

        uint64_t* out = nullptr;
        checkCuda(cudaMalloc(&out, 5 * sizeof(uint64_t)), "cudaMalloc out");
        checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                  "cudaMemsetAsync out");
        uint64_t host[5] = {};
        if (!FLAGS_loopback) {
            probeKernel<<<1, 1, 0, stream>>>(
                static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr()), out);
            checkCuda(cudaGetLastError(), "probeKernel launch");
            checkCuda(cudaStreamSynchronize(stream), "probeKernel sync");

            checkCuda(cudaMemcpy(host, out, sizeof(host), cudaMemcpyDeviceToHost),
                      "cudaMemcpy out");
            check(host[3] == 0x1122334455667788ULL, "WQ write/read mismatch");
            check(static_cast<uint32_t>(host[4]) == 0x01020304U,
                  "DBR write/read mismatch");
        }

        if (FLAGS_loopback) {
            uint64_t* buf = nullptr;
            checkCuda(cudaMalloc(&buf, 2 * sizeof(uint64_t)), "cudaMalloc buf");
            const uint64_t init[2] = {0xaabbccddeeff0011ULL, 0};
            checkCuda(cudaMemcpy(buf, init, sizeof(init), cudaMemcpyHostToDevice),
                      "cudaMemcpy init");
            checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                      "cudaMemsetAsync before registerMemory");
            checkCuda(cudaStreamSynchronize(stream),
                      "cudaStreamSynchronize before registerMemory");
            check(rdma->registerMemory(buf, 2 * sizeof(uint64_t)) == 0,
                  "RdmaTransport::registerMemory failed");
            checkCuda(cudaGetLastError(), "cudaGetLastError after registerMemory");
            checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                      "cudaMemsetAsync after registerMemory");
            checkCuda(cudaStreamSynchronize(stream),
                      "cudaStreamSynchronize after registerMemory");

            auto meta = rdma->localMetadata();
            check(meta.qpns.size() == 1, "unexpected qpn count");
            std::vector<int64_t> addrs{meta.raddr};
            std::vector<int32_t> keys{meta.rkey};
            std::vector<int32_t> qpns{meta.qpns[0]};
            std::vector<int32_t> lids{meta.lids[0]};
            std::vector<int64_t> subnet_prefixes{meta.subnet_prefix};
            std::vector<int64_t> interface_ids{meta.interface_id};
            std::vector<int> active{1};
            check(rdma->connectPeers(0, rdma->isRoce(), addrs, keys, qpns, lids,
                                     subnet_prefixes, interface_ids, active) == 0,
                  "RdmaTransport::connectPeers failed");
            checkCuda(cudaGetLastError(), "cudaGetLastError after connectPeers");

            checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                      "cudaMemsetAsync loopback out");
            loopbackKernel<<<1, 1, 0, stream>>>(
                static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr()),
                reinterpret_cast<uint64_t>(buf),
                reinterpret_cast<uint64_t>(buf + 1),
                static_cast<uint32_t>(meta.rkey), out);
            checkCuda(cudaGetLastError(), "loopbackKernel launch");
            checkCuda(cudaStreamSynchronize(stream), "loopbackKernel sync");

            uint64_t result[2] = {};
            checkCuda(cudaMemcpy(result, buf, sizeof(result),
                                 cudaMemcpyDeviceToHost),
                      "cudaMemcpy result");
            cudaFree(buf);
            check(result[1] == init[0], "RDMA loopback data mismatch");
        }

        if (FLAGS_loopback) {
            checkCuda(cudaMemcpy(host, out, sizeof(host), cudaMemcpyDeviceToHost),
                      "cudaMemcpy loopback out");
        }
        cudaFree(out);
        cudaStreamDestroy(stream);

        std::cout << "device_ibgda_devctx_probe passed"
                  << " wq=0x" << std::hex << host[0]
                  << " dbr=0x" << host[1]
                  << " bf=0x" << host[2] << std::dec << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "device_ibgda_devctx_probe failed: " << e.what() << "\n";
        return 1;
    }
}
