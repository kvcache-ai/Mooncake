#include <gflags/gflags.h>

#include <cstdint>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/stat.h>

#include "cuda_alike.h"
#include "transport/device/device_ops.cuh"
#include "transport/device/device_transport.h"
#include "transport/device/ibgda_device.cuh"
#include "transport/device/ibgda/mlx5gda.h"

DEFINE_string(device, "", "RDMA device name, e.g. mlx5_10. Empty means auto");
DEFINE_int32(gpu_id, 0, "CUDA-like device id");
DEFINE_int32(rank, 0, "Rank for production-path 2-rank probe");
DEFINE_int32(world_size, 1, "1 for self-loopback, 2 for rank0->rank1");
DEFINE_string(ipc_dir, "/tmp/mooncake-ibgda-devctx-probe",
              "Directory for 2-rank metadata exchange");
DEFINE_bool(loopback, false, "Post a self RDMA WRITE from device kernel");
DEFINE_bool(host_wqe_device_db, false,
            "Host writes WQE; device kernel only updates DBR and rings UAR");
DEFINE_bool(uar_only, false, "Device kernel only writes UAR offset 0");
DEFINE_bool(device_api_put, false,
            "Issue loopback RDMA WRITE through mc_ibgda_put");
DEFINE_bool(device_api_red_add, false,
            "Issue RDMA masked atomic add through mc_ibgda_red_add");

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

__global__ void deviceApiPutKernel(mooncake::device::IbgdaContext ctx,
                                   int src_rank, int dst_rank,
                                   uint64_t src, uint64_t dst, uint64_t* out) {
    mooncake::device::mc_ibgda_put(ctx, 0, dst_rank, src_rank, 1,
                                   reinterpret_cast<const void*>(src), dst,
                                   sizeof(uint64_t));
    out[0] = 1;
}

__global__ void deviceApiRedAddKernel(mooncake::device::IbgdaContext ctx,
                                      int src_rank, int dst_rank,
                                      uint64_t scratch, uint64_t dst,
                                      int32_t value, uint64_t* out) {
    mooncake::device::mc_ibgda_red_add(ctx, 0, dst_rank, src_rank, 1, scratch,
                                       dst, value);
    out[0] = 2;
}

__global__ void ringDoorbellOnlyKernel(mlx5gda_qp_devctx* qps, uint64_t* out) {
    mlx5gda_qp_devctx* qp = qps;
    ++qp->wq_head;
    uint32_t num_posted = static_cast<uint32_t>(qp->wq_head);
    mooncake::device::mc_st_release_u32(
        reinterpret_cast<uint32_t*>(&qp->dbr->send_counter),
        mooncake::device::mc_bswap32(num_posted));
    if (qp->bf != nullptr) {
        auto* wqe = qp->wq + ((num_posted - 1) & qp->wqeid_mask);
        mooncake::device::mc_st_release_u64(
            reinterpret_cast<uint64_t*>(qp->bf), wqe->qwords[0]);
    }
    out[0] = qp->qpn;
    out[1] = qp->wq_head;
    out[2] = reinterpret_cast<uint64_t>(qp->bf);
}

__global__ void writeUarOnlyKernel(mlx5gda_qp_devctx* qps, uint64_t* out) {
    mlx5gda_qp_devctx* qp = qps;
    out[0] = reinterpret_cast<uint64_t>(qp->bf);
    if (qp->bf != nullptr) {
        mooncake::device::mc_st_release_u64(
            reinterpret_cast<uint64_t*>(qp->bf), 0);
    }
    out[1] = 1;
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

uint32_t hostBswap32(uint32_t x) { return __builtin_bswap32(x); }
uint64_t hostBswap64(uint64_t x) { return __builtin_bswap64(x); }

struct RankMeta {
    int64_t raddr;
    int32_t rkey;
    int64_t subnet_prefix;
    int64_t interface_id;
    int32_t qpns[2];
    int32_t lids[2];
};

std::string metaPath(int rank) {
    return FLAGS_ipc_dir + "/rank" + std::to_string(rank) + ".meta";
}

std::string donePath() { return FLAGS_ipc_dir + "/rank0.done"; }

void writeMeta(int rank, const RankMeta& meta) {
    mkdir(FLAGS_ipc_dir.c_str(), 0755);
    std::ofstream ofs(metaPath(rank), std::ios::binary | std::ios::trunc);
    check(static_cast<bool>(ofs), "open meta for write failed");
    ofs.write(reinterpret_cast<const char*>(&meta), sizeof(meta));
}

RankMeta readMeta(int rank) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream ifs(metaPath(rank), std::ios::binary);
        if (ifs) {
            RankMeta meta{};
            ifs.read(reinterpret_cast<char*>(&meta), sizeof(meta));
            if (ifs.gcount() == static_cast<std::streamsize>(sizeof(meta))) {
                return meta;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    throw std::runtime_error("timeout waiting for peer metadata");
}

void writeDone() {
    std::ofstream ofs(donePath(), std::ios::trunc);
    check(static_cast<bool>(ofs), "open done for write failed");
    ofs << "done\n";
}

void waitDone() {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream ifs(donePath());
        if (ifs) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    throw std::runtime_error("timeout waiting for rank0 done");
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try {
        int device_count = 0;
        checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
        check(FLAGS_world_size == 1 || FLAGS_world_size == 2,
              "--world_size must be 1 or 2");
        check(FLAGS_rank >= 0 && FLAGS_rank < FLAGS_world_size,
              "invalid --rank");
        check(FLAGS_gpu_id >= 0 && FLAGS_gpu_id < device_count,
              "invalid --gpu_id");
        checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice");

        auto rdma = mooncake::device::createIbgdaDeviceTransport();
        check(static_cast<bool>(rdma),
              "createIbgdaDeviceTransport returned null");
        int num_ranks = FLAGS_world_size;
        int num_qps = FLAGS_world_size;
        check(rdma->initialize(FLAGS_device, num_ranks, num_qps) == 0,
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
        if (FLAGS_uar_only) {
            writeUarOnlyKernel<<<1, 1, 0, stream>>>(
                static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr()), out);
            checkCuda(cudaGetLastError(), "writeUarOnlyKernel launch");
            checkCuda(cudaStreamSynchronize(stream), "writeUarOnlyKernel sync");
            checkCuda(cudaMemcpy(host, out, sizeof(host), cudaMemcpyDeviceToHost),
                      "cudaMemcpy uar-only out");
        } else if (!FLAGS_loopback && !FLAGS_host_wqe_device_db &&
                   !FLAGS_device_api_put && !FLAGS_device_api_red_add) {
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

        if (FLAGS_loopback || FLAGS_host_wqe_device_db || FLAGS_device_api_put ||
            FLAGS_device_api_red_add) {
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
            check(static_cast<int>(meta.qpns.size()) == num_qps,
                  "unexpected qpn count");

            RankMeta local{};
            local.raddr = meta.raddr;
            local.rkey = meta.rkey;
            local.subnet_prefix = meta.subnet_prefix;
            local.interface_id = meta.interface_id;
            for (int i = 0; i < num_qps; ++i) {
                local.qpns[i] = meta.qpns[i];
                local.lids[i] = meta.lids[i];
            }
            RankMeta peer = local;
            if (FLAGS_world_size == 2) {
                writeMeta(FLAGS_rank, local);
                peer = readMeta(1 - FLAGS_rank);
            }

            std::vector<RankMeta> metas{local};
            if (FLAGS_world_size == 2) {
                metas.resize(2);
                metas[FLAGS_rank] = local;
                metas[1 - FLAGS_rank] = peer;
            }

            std::vector<int64_t> addrs(num_ranks);
            std::vector<int32_t> keys(num_ranks);
            std::vector<int64_t> subnet_prefixes(num_ranks);
            std::vector<int64_t> interface_ids(num_ranks);
            for (int r = 0; r < num_ranks; ++r) {
                addrs[r] = metas[r].raddr;
                keys[r] = metas[r].rkey;
                subnet_prefixes[r] = metas[r].subnet_prefix;
                interface_ids[r] = metas[r].interface_id;
            }
            std::vector<int32_t> qpns(num_qps);
            std::vector<int32_t> lids(num_qps);
            int qps_per_rank = num_qps / num_ranks;
            for (int i = 0; i < num_qps; ++i) {
                int peer_rank = i * num_ranks / num_qps;
                int channel = i % qps_per_rank;
                int peer_qp_idx = FLAGS_rank * qps_per_rank + channel;
                qpns[i] = metas[peer_rank].qpns[peer_qp_idx];
                lids[i] = metas[peer_rank].lids[peer_qp_idx];
            }
            std::vector<int> active(num_ranks, 1);
            check(rdma->connectPeers(FLAGS_rank, rdma->isRoce(), addrs, keys,
                                     qpns, lids, subnet_prefixes,
                                     interface_ids, active) == 0,
                  "RdmaTransport::connectPeers failed");
            checkCuda(cudaGetLastError(), "cudaGetLastError after connectPeers");

            bool is_initiator = FLAGS_world_size == 1 || FLAGS_rank == 0;
            int dst_rank = FLAGS_world_size == 1 ? 0 : 1;
            uint64_t remote_dst =
                static_cast<uint64_t>(addrs[dst_rank]) + sizeof(uint64_t);

            if (FLAGS_device_api_put && is_initiator) {
                mooncake::device::IbgdaContext ctx{};
                ctx.qp_devctxs =
                    static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr());
                ctx.raddrs = static_cast<const uint64_t*>(rdma->raddrsPtr());
                ctx.rkeys = static_cast<const uint32_t*>(rdma->rkeysPtr());
                ctx.local_atomic_base = buf;
                ctx.remote_atomic_base = buf;
                deviceApiPutKernel<<<1, 1, 0, stream>>>(
                    ctx, FLAGS_rank, dst_rank, reinterpret_cast<uint64_t>(buf),
                    remote_dst, out);
                checkCuda(cudaGetLastError(), "deviceApiPutKernel launch");
                checkCuda(cudaStreamSynchronize(stream),
                          "deviceApiPutKernel sync");
            } else if (FLAGS_device_api_red_add && is_initiator) {
                mooncake::device::IbgdaContext ctx{};
                ctx.qp_devctxs =
                    static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr());
                ctx.raddrs = static_cast<const uint64_t*>(rdma->raddrsPtr());
                ctx.rkeys = static_cast<const uint32_t*>(rdma->rkeysPtr());
                ctx.local_atomic_base = buf;
                ctx.remote_atomic_base = buf;
                deviceApiRedAddKernel<<<1, 1, 0, stream>>>(
                    ctx, FLAGS_rank, dst_rank, reinterpret_cast<uint64_t>(buf),
                    remote_dst, 7, out);
                checkCuda(cudaGetLastError(), "deviceApiRedAddKernel launch");
                checkCuda(cudaStreamSynchronize(stream),
                          "deviceApiRedAddKernel sync");
            } else if ((FLAGS_device_api_put || FLAGS_device_api_red_add) &&
                       !is_initiator) {
                // Passive rank: QPs are connected and the proxy worker is
                // running, but this rank only verifies the remote write/atomic.
            } else if (FLAGS_host_wqe_device_db) {
                mlx5gda_qp_devctx devctx{};
                checkCuda(cudaMemcpy(&devctx, rdma->qpDevCtxsPtr(),
                                     sizeof(devctx), cudaMemcpyDeviceToHost),
                          "cudaMemcpy devctx");
                auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(
                    devctx.wq + (devctx.wq_head & devctx.wqeid_mask));
                *wqe = {};
                wqe->ctrl.qpn_ds = hostBswap32((devctx.qpn << 8) | 3);
                wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
                wqe->ctrl.opmod_idx_opcode = hostBswap32(MLX5_OPCODE_RDMA_WRITE);
                wqe->raddr.raddr =
                    hostBswap64(reinterpret_cast<uint64_t>(buf + 1));
                wqe->raddr.rkey = hostBswap32(static_cast<uint32_t>(meta.rkey));
                wqe->data.byte_count = hostBswap32(sizeof(uint64_t));
                wqe->data.lkey = hostBswap32(static_cast<uint32_t>(meta.rkey));
                wqe->data.addr = hostBswap64(reinterpret_cast<uint64_t>(buf));

                checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                          "cudaMemsetAsync host_wqe_device_db out");
                ringDoorbellOnlyKernel<<<1, 1, 0, stream>>>(
                    static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr()), out);
                checkCuda(cudaGetLastError(), "ringDoorbellOnlyKernel launch");
                checkCuda(cudaStreamSynchronize(stream),
                          "ringDoorbellOnlyKernel sync");
            } else {
                checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                          "cudaMemsetAsync loopback out");
                loopbackKernel<<<1, 1, 0, stream>>>(
                    static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr()),
                    reinterpret_cast<uint64_t>(buf),
                    reinterpret_cast<uint64_t>(buf + 1),
                    static_cast<uint32_t>(meta.rkey), out);
                checkCuda(cudaGetLastError(), "loopbackKernel launch");
                checkCuda(cudaStreamSynchronize(stream), "loopbackKernel sync");
            }

            if (FLAGS_world_size == 2 && FLAGS_rank == 0) {
                writeDone();
            } else if (FLAGS_world_size == 2) {
                waitDone();
            }

            uint64_t result[2] = {};
            uint64_t expected = init[0];
            if (FLAGS_device_api_red_add) expected = 7;
            int verify_rank = FLAGS_world_size == 1 ? FLAGS_rank : dst_rank;
            bool should_verify = FLAGS_rank == verify_rank ||
                                 FLAGS_loopback || FLAGS_host_wqe_device_db;
            auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(1);
            do {
                checkCuda(cudaMemcpy(result, buf, sizeof(result),
                                     cudaMemcpyDeviceToHost),
                          "cudaMemcpy result");
                if (!should_verify || result[1] == expected) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (std::chrono::steady_clock::now() < deadline);
            cudaFree(buf);
            if (should_verify) {
                check(result[1] == expected, "RDMA result mismatch");
            }
        }

        if (FLAGS_loopback || FLAGS_host_wqe_device_db ||
            FLAGS_device_api_put || FLAGS_device_api_red_add) {
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
