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
DEFINE_int32(requests, 1,
             "Number of Device API operations issued by the initiator");
DEFINE_int32(num_qps, 0,
             "Number of QPs to create. 0 means world_size");
DEFINE_int32(active_qps_per_rank, 0,
             "Number of per-peer QP channels to actively use. 0 means all");
DEFINE_bool(bidirectional, false,
            "In 2-rank mode, both ranks issue Device API operations");
DEFINE_int32(dst_word_offset, -1,
             "Destination word offset inside the registered buffer. "
             "-1 means requests, matching the original second-half layout");
DEFINE_int32(bytes_per_request, 8,
             "Bytes per Device API put request. Must be a multiple of 8");
DEFINE_bool(kernel_write_source, false,
            "Have the device kernel write source bytes immediately before "
            "issuing mc_ibgda_put");
DEFINE_bool(register_before_qp, false,
            "Diagnostic: register payload memory before allocating control "
            "buffer and creating QPs, matching EP initialization order");
DEFINE_int32(buffer_words, 0,
             "Diagnostic: total uint64 words to allocate/register. 0 means "
             "minimum required by source and destination layout");

namespace {

constexpr int kMaxProbeQps = 512;

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
                                   int qps_per_rank,
                                   int active_qps_per_rank,
                                   uint64_t src, uint64_t dst,
                                   int requests, uint32_t bytes_per_request,
                                   bool kernel_write_source,
                                   uint64_t* out) {
    int words_per_request = bytes_per_request / sizeof(uint64_t);
    for (int i = 0; i < requests; ++i) {
        if (kernel_write_source) {
            auto* src_words = reinterpret_cast<uint64_t*>(
                src + static_cast<uint64_t>(i) * bytes_per_request);
            for (int w = 0; w < words_per_request; ++w) {
                src_words[w] = 0xaabbccddeeff0011ULL +
                               static_cast<uint64_t>(
                                   i * words_per_request + w);
            }
            mooncake::device::mc_fence();
        }
        if (i == 0) {
            int channel = i % active_qps_per_rank;
            int qp_idx = dst_rank * qps_per_rank + (channel % qps_per_rank);
            auto* qp = ctx.qp_devctxs + qp_idx;
            uint64_t laddr = src + static_cast<uint64_t>(i) * bytes_per_request;
            uint64_t raddr = dst + static_cast<uint64_t>(i) * bytes_per_request;
            printf("[TE probe put] src_rank=%d dst_rank=%d channel=%d "
                   "qps_per_rank=%d qp_idx=%d qpn=%u laddr=0x%llx "
                   "lkey=0x%x raddr=0x%llx rkey=0x%x bytes=%u source=0x%llx\n",
                   src_rank, dst_rank, channel, qps_per_rank, qp_idx, qp->qpn,
                   static_cast<unsigned long long>(laddr),
                   ctx.rkeys[src_rank],
                   static_cast<unsigned long long>(raddr),
                   ctx.rkeys[dst_rank], bytes_per_request,
                   static_cast<unsigned long long>(
                       *reinterpret_cast<uint64_t*>(laddr)));
        }
        mooncake::device::mc_ibgda_put(
            ctx, i % active_qps_per_rank, dst_rank, src_rank, qps_per_rank,
            reinterpret_cast<const void*>(src +
                                          i * bytes_per_request),
            dst + i * bytes_per_request, bytes_per_request);
    }
    out[0] = requests;
}

__global__ void deviceApiRedAddKernel(mooncake::device::IbgdaContext ctx,
                                      int src_rank, int dst_rank,
                                      int qps_per_rank,
                                      int active_qps_per_rank,
                                      uint64_t scratch, uint64_t dst,
                                      int32_t value, int requests,
                                      uint64_t* out) {
    for (int i = 0; i < requests; ++i) {
        mooncake::device::mc_ibgda_red_add(
            ctx, i % active_qps_per_rank, dst_rank, src_rank, qps_per_rank,
            scratch + i * sizeof(uint64_t), dst + i * sizeof(uint64_t),
            value);
    }
    out[0] = requests;
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
    int32_t num_qps;
    int32_t qpns[kMaxProbeQps];
    int32_t lids[kMaxProbeQps];
};

std::string metaPath(int rank) {
    return FLAGS_ipc_dir + "/rank" + std::to_string(rank) + ".meta";
}

std::string donePath() { return FLAGS_ipc_dir + "/rank0.done"; }
std::string donePath(int rank) {
    return FLAGS_ipc_dir + "/rank" + std::to_string(rank) + ".done";
}

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

void writeDone(int rank) {
    std::ofstream ofs(donePath(rank), std::ios::trunc);
    check(static_cast<bool>(ofs), "open rank done for write failed");
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

void waitDone(int rank) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream ifs(donePath(rank));
        if (ifs) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    throw std::runtime_error("timeout waiting for rank done");
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
        check(FLAGS_requests > 0, "--requests must be positive");
        check(FLAGS_bytes_per_request > 0 &&
                  FLAGS_bytes_per_request % sizeof(uint64_t) == 0,
              "--bytes_per_request must be a positive multiple of 8");
        check(FLAGS_gpu_id >= 0 && FLAGS_gpu_id < device_count,
              "invalid --gpu_id");
        checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice");

        auto rdma = mooncake::device::createIbgdaDeviceTransport();
        check(static_cast<bool>(rdma),
              "createIbgdaDeviceTransport returned null");
        int num_ranks = FLAGS_world_size;
        int num_qps = FLAGS_num_qps == 0 ? FLAGS_world_size : FLAGS_num_qps;
        check(num_qps >= num_ranks && num_qps % num_ranks == 0,
              "--num_qps must be >= world_size and divisible by world_size");
        check(num_qps <= kMaxProbeQps, "--num_qps exceeds kMaxProbeQps");
        check(rdma->initialize(FLAGS_device, num_ranks, num_qps) == 0,
              "RdmaTransport::initialize failed");

        cudaStream_t stream = nullptr;
        checkCuda(cudaStreamCreate(&stream), "cudaStreamCreate");
        if (!FLAGS_register_before_qp) {
            check(rdma->allocateControlBuffer() == 0,
                  "RdmaTransport::allocateControlBuffer failed");
            check(rdma->createQueuePairs(reinterpret_cast<void*>(stream)) == 0,
                  "RdmaTransport::createQueuePairs failed");
        }

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
            int words_per_request =
                FLAGS_bytes_per_request / static_cast<int>(sizeof(uint64_t));
            int src_words =
                FLAGS_requests * words_per_request;
            int dst_word_offset = FLAGS_dst_word_offset < 0 ? src_words
                                                            : FLAGS_dst_word_offset;
            check(dst_word_offset >= src_words,
                  "--dst_word_offset must not overlap source words");
            size_t words = static_cast<size_t>(dst_word_offset) +
                           static_cast<size_t>(src_words);
            if (FLAGS_buffer_words > 0) {
                check(FLAGS_buffer_words >= static_cast<int>(words),
                      "--buffer_words must cover source and destination");
                words = static_cast<size_t>(FLAGS_buffer_words);
            }
            uint64_t* buf = nullptr;
            std::vector<uint64_t> init(words, 0);
            for (int i = 0; i < src_words; ++i) {
                init[i] = 0xaabbccddeeff0011ULL + static_cast<uint64_t>(i);
            }
            checkCuda(cudaMalloc(&buf, words * sizeof(uint64_t)),
                      "cudaMalloc buf");
            checkCuda(cudaMemcpy(buf, init.data(), words * sizeof(uint64_t),
                                 cudaMemcpyHostToDevice),
                      "cudaMemcpy init");
            checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                      "cudaMemsetAsync before registerMemory");
            checkCuda(cudaStreamSynchronize(stream),
                      "cudaStreamSynchronize before registerMemory");
            check(rdma->registerMemory(buf, words * sizeof(uint64_t)) == 0,
                  "RdmaTransport::registerMemory failed");
            checkCuda(cudaGetLastError(), "cudaGetLastError after registerMemory");
            checkCuda(cudaMemsetAsync(out, 0, 5 * sizeof(uint64_t), stream),
                      "cudaMemsetAsync after registerMemory");
            checkCuda(cudaStreamSynchronize(stream),
                      "cudaStreamSynchronize after registerMemory");
            if (FLAGS_register_before_qp) {
                check(rdma->allocateControlBuffer() == 0,
                      "RdmaTransport::allocateControlBuffer failed");
                check(rdma->createQueuePairs(reinterpret_cast<void*>(stream)) == 0,
                      "RdmaTransport::createQueuePairs failed");
            }

            auto meta = rdma->localMetadata();
            check(static_cast<int>(meta.qpns.size()) == num_qps,
                  "unexpected qpn count");

            RankMeta local{};
            local.raddr = meta.raddr;
            local.rkey = meta.rkey;
            local.subnet_prefix = meta.subnet_prefix;
            local.interface_id = meta.interface_id;
            local.num_qps = num_qps;
            for (int i = 0; i < num_qps; ++i) {
                local.qpns[i] = meta.qpns[i];
                local.lids[i] = meta.lids[i];
            }
            RankMeta peer = local;
            if (FLAGS_world_size == 2) {
                writeMeta(FLAGS_rank, local);
                peer = readMeta(1 - FLAGS_rank);
                check(peer.num_qps == num_qps, "peer num_qps mismatch");
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
            int active_qps_per_rank =
                FLAGS_active_qps_per_rank == 0 ? qps_per_rank
                                                : FLAGS_active_qps_per_rank;
            check(active_qps_per_rank > 0 &&
                      active_qps_per_rank <= qps_per_rank,
                  "--active_qps_per_rank must be in [1, num_qps/world_size]");
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

            bool is_initiator = FLAGS_world_size == 1 || FLAGS_rank == 0 ||
                                FLAGS_bidirectional;
            int dst_rank = FLAGS_world_size == 1 ? 0 : 1;
            if (FLAGS_world_size == 2 && FLAGS_bidirectional)
                dst_rank = 1 - FLAGS_rank;
            uint64_t remote_dst =
                static_cast<uint64_t>(addrs[dst_rank]) +
                static_cast<uint64_t>(dst_word_offset) * sizeof(uint64_t);

            if (FLAGS_device_api_put && is_initiator) {
                mooncake::device::IbgdaContext ctx{};
                ctx.qp_devctxs =
                    static_cast<mlx5gda_qp_devctx*>(rdma->qpDevCtxsPtr());
                ctx.raddrs = static_cast<const uint64_t*>(rdma->raddrsPtr());
                ctx.rkeys = static_cast<const uint32_t*>(rdma->rkeysPtr());
                ctx.local_atomic_base = buf;
                ctx.remote_atomic_base = buf;
                deviceApiPutKernel<<<1, 1, 0, stream>>>(
                    ctx, FLAGS_rank, dst_rank, qps_per_rank,
                    active_qps_per_rank,
                    reinterpret_cast<uint64_t>(buf), remote_dst,
                    FLAGS_requests,
                    static_cast<uint32_t>(FLAGS_bytes_per_request),
                    FLAGS_kernel_write_source, out);
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
                    ctx, FLAGS_rank, dst_rank, qps_per_rank,
                    active_qps_per_rank,
                    reinterpret_cast<uint64_t>(buf), remote_dst, 7,
                    FLAGS_requests, out);
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

            if (FLAGS_world_size == 2 && FLAGS_bidirectional) {
                writeDone(FLAGS_rank);
                waitDone(1 - FLAGS_rank);
            } else if (FLAGS_world_size == 2 && FLAGS_rank == 0) {
                writeDone();
            } else if (FLAGS_world_size == 2) {
                waitDone();
            }

            int verify_rank = FLAGS_world_size == 1 ? FLAGS_rank : dst_rank;
            bool should_verify = (FLAGS_bidirectional && FLAGS_world_size == 2) ||
                                 FLAGS_rank == verify_rank ||
                                 FLAGS_loopback || FLAGS_host_wqe_device_db;
            auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(1);
            std::vector<uint64_t> result(words, 0);
            bool matched = false;
            int first_mismatch = -1;
            uint64_t expected_value = 0;
            uint64_t actual_value = 0;
            do {
                checkCuda(cudaMemcpy(result.data(), buf,
                                     words * sizeof(uint64_t),
                                     cudaMemcpyDeviceToHost),
                          "cudaMemcpy result");
                matched = true;
                if (FLAGS_device_api_red_add) {
                    for (int i = 0; i < FLAGS_requests; ++i) {
                        uint64_t expected = 7;
                        uint64_t actual = result[dst_word_offset + i];
                        if (actual != expected) {
                            matched = false;
                            if (first_mismatch < 0) {
                                first_mismatch = i;
                                expected_value = expected;
                                actual_value = actual;
                            }
                        }
                    }
                } else if (FLAGS_device_api_put) {
                    for (int i = 0; i < src_words; ++i) {
                        uint64_t expected = init[i];
                        uint64_t actual = result[dst_word_offset + i];
                        if (actual != expected) {
                            matched = false;
                            if (first_mismatch < 0) {
                                first_mismatch = i;
                                expected_value = expected;
                                actual_value = actual;
                            }
                        }
                    }
                } else {
                    matched = result[1] == init[0];
                    if (!matched) {
                        first_mismatch = 1;
                        expected_value = init[0];
                        actual_value = result[1];
                    }
                }
                if (!should_verify || matched) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (std::chrono::steady_clock::now() < deadline);
            cudaFree(buf);
            if (should_verify) {
                if (!matched) {
                    std::cerr << "first mismatch rank=" << FLAGS_rank
                              << " verify_rank=" << verify_rank
                              << " index=" << first_mismatch
                              << " expected=0x" << std::hex << expected_value
                              << " actual=0x" << actual_value << std::dec
                              << " num_qps=" << num_qps
                              << " qps_per_rank=" << qps_per_rank
                              << " active_qps_per_rank="
                              << active_qps_per_rank
                              << " dst_word_offset=" << dst_word_offset
                              << " bytes_per_request="
                              << FLAGS_bytes_per_request
                              << " requests=" << FLAGS_requests << "\n";
                }
                check(matched, "RDMA result mismatch");
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
