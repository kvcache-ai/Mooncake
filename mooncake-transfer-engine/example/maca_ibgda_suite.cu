#include <cuda_runtime.h>
#include <glog/logging.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>
#include "transport/device/ibgda_device.cuh"
#include "transport/device/device_transport.h"

using namespace mooncake::device;
constexpr size_t REGION = 4 * 1024 * 1024;
constexpr size_t TOTAL = 2 * REGION;
struct Result {
    int status;
    unsigned expected, counter, owner, completed;
};
constexpr int CHANNELS = 4;
struct Wire {
    int64_t addr, subnet, iface;
    int32_t key, qp[2 * CHANNELS], lid[2 * CHANNELS];
};
struct Case {
    const char* name;
    int bytes, count, burst, mode;
};
// Modes: 0 WRITE, 1 atomic +1, 2 contended WRITE, 3 GPU publish/ack,
// 4 atomic -1, 5 contended multi-QP WRITE. See maca_ibgda.md for the contract.
__host__ __device__ unsigned char pattern(int rank, int epoch, int slot,
                                          int byte) {
    unsigned x = 0x9e3779b9u * (slot + 1) + 0x85ebca6bu * (epoch + 1);
    x ^= unsigned(byte) * 0xc2b2ae35u + unsigned(rank) * 0x27d4eb2du;
    x ^= x >> 16;
    return static_cast<unsigned char>(x);
}
__device__ bool await_cq(mlx5gda_qp_devctx* qp, uint16_t expected, Result* r) {
    auto start = clock64();
    while (clock64() - start < 2000000000ULL) {
        unsigned owner = *reinterpret_cast<volatile uint8_t*>(&qp->cq->op_own);
        unsigned counter = mc_bswap16(
            *reinterpret_cast<volatile uint16_t*>(&qp->cq->wqe_counter));
        r->expected = expected;
        r->counter = counter;
        r->owner = owner;
        if ((owner >> 4) != 0 && (owner >> 4) != 0xf) {
            r->status = -2;
            return false;
        }
        // Collapsed CQ: the counter identifies completion; no ordinary ring
        // consumer DBR.
        if ((owner >> 4) == 0 && counter == expected) {
            mc_ibgda_poll_cq(qp, expected);
            return true;
        }
    }
    r->status = -1;
    return false;
}
__device__ bool await_signal(const int* ptr, int expected, Result* r) {
    auto start = clock64();
    while (mc_ld_acquire(ptr) != expected) {
        if (clock64() - start > 2000000000ULL) {
            r->status = -3;
            return false;
        }
    }
    __threadfence();
    __threadfence_system();
    return true;
}
__global__ void transfer(IbgdaContext ctx, char* buf, uint64_t remote, int rank,
                         int epoch, Case c, Result* r) {
    if (threadIdx.x != 0) return;
    auto* qp = ctx.qp_devctxs + (1 - rank) * CHANNELS;
    if (c.mode == 3) {
        r->status = 0;
        r->completed = 0;
        for (int round = 0; round < c.count; ++round) {
            for (int j = 0; j < c.bytes; ++j)
                buf[j] = pattern(rank, epoch, round, j);
            __threadfence();
            __threadfence_system();
            mc_ibgda_put(ctx, 0, 1 - rank, rank, CHANNELS, buf, remote + REGION,
                         c.bytes);
            mc_ibgda_red_add(ctx, 0, 1 - rank, rank, CHANNELS,
                             reinterpret_cast<uint64_t>(buf + REGION - 32),
                             remote + TOTAL - 16, 1);
            if (!await_cq(qp, static_cast<uint16_t>(qp->wq_head - 1), r))
                return;
            if (!await_signal(reinterpret_cast<int*>(buf + TOTAL - 16),
                              round + 1, r))
                return;
            for (int j = 0; j < c.bytes; ++j) {
                if (static_cast<unsigned char>(buf[REGION + j]) !=
                    pattern(1 - rank, epoch, round, j)) {
                    r->status = -4;
                    r->completed = round;
                    return;
                }
            }
            mc_ibgda_red_add(ctx, 0, 1 - rank, rank, CHANNELS,
                             reinterpret_cast<uint64_t>(buf + REGION - 24),
                             remote + TOTAL - 8, 1);
            if (!await_cq(qp, static_cast<uint16_t>(qp->wq_head - 1), r))
                return;
            if (!await_signal(reinterpret_cast<int*>(buf + TOTAL - 8),
                              round + 1, r))
                return;
            r->completed = round + 1;
        }
        r->status = 1;
        return;
    }
    if (c.mode == 2 || c.mode == 5) {
        for (int slot = blockIdx.x; slot < c.count; slot += gridDim.x) {
            char* src = buf + size_t(slot) * c.bytes;
            for (int j = 0; j < c.bytes; ++j)
                src[j] = pattern(rank, epoch, slot, j);
            __threadfence();
            __threadfence_system();
            mc_ibgda_put(ctx, c.mode == 5 ? slot % CHANNELS : 0, 1 - rank, rank,
                         CHANNELS, src,
                         remote + REGION + size_t(slot) * c.bytes, c.bytes);
        }
        return;
    }
    r->status = 0;
    r->completed = 0;
    for (int slot = 0; slot < c.count; ++slot) {
        if (c.mode == 1 || c.mode == 4) {
            mc_ibgda_red_add(ctx, 0, 1 - rank, rank, CHANNELS,
                             reinterpret_cast<uint64_t>(buf + size_t(slot) * 8),
                             remote + REGION, c.mode == 4 ? -1 : 1);
        } else {
            char* src = buf + size_t(slot) * c.bytes;
            for (int j = 0; j < c.bytes; ++j)
                src[j] = pattern(rank, epoch, slot, j);
            __threadfence();
            __threadfence_system();
            mc_ibgda_put(ctx, 0, 1 - rank, rank, CHANNELS, src,
                         remote + REGION + size_t(slot) * c.bytes, c.bytes);
        }
        if ((slot + 1) % c.burst == 0 || slot + 1 == c.count) {
            if (!await_cq(qp, static_cast<uint16_t>(qp->wq_head - 1), r))
                return;
            r->completed = slot + 1;
        }
    }
    r->status = 1;
}
__global__ void finish(IbgdaContext ctx, int rank, int channels, Result* r) {
    if (threadIdx.x || blockIdx.x) return;
    for (int i = 0; i < channels; ++i) {
        auto* qp = ctx.qp_devctxs + (1 - rank) * CHANNELS + i;
        if (!await_cq(qp, static_cast<uint16_t>(qp->wq_head - 1), r)) return;
    }
    r->status = 1;
}
static void check(cudaError_t e) {
    if (e != cudaSuccess) throw std::runtime_error(cudaGetErrorString(e));
}
static void barrier(const std::string& base, int rank,
                    const std::string& phase) {
    auto path = base + "." + phase;
    std::ofstream(path + std::to_string(rank)).put('1');
    auto start = std::chrono::steady_clock::now();
    while (!std::ifstream(path + std::to_string(1 - rank)).good()) {
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(30))
            throw std::runtime_error("barrier timeout: " + phase);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
int main(int argc, char** argv) try {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " RANK GPU NIC EXCHANGE_PREFIX\n";
        return 2;
    }
    int rank = std::stoi(argv[1]);
    int gpu = std::stoi(argv[2]);
    if (rank < 0 || rank > 1) return 2;
    std::string base = argv[4];
    google::InitGoogleLogging(argv[0]);
    check(cudaSetDevice(gpu));
    const char* nic = argv[3];
    char* buf = nullptr;
    check(mcExtMallocWithFlags(reinterpret_cast<void**>(&buf), TOTAL,
                               mcDeviceMallocFinegrained));
    auto t = createIbgdaDeviceTransport({nic});
    if (t->initialize(nic, 2, 2 * CHANNELS) || t->allocateControlBuffer() ||
        t->createQueuePairs(nullptr) || t->registerMemory(buf, TOTAL))
        throw std::runtime_error("bootstrap failed");
    Wire peer{};
    auto exchange_connect = [&](const std::string& phase) {
        auto m = t->localMetadata();
        Wire self{m.raddr, m.subnet_prefix, m.interface_id, m.rkey, {}, {}};
        for (int i = 0; i < 2 * CHANNELS; ++i) {
            self.qp[i] = m.qpns[i];
            self.lid[i] = m.lids[i];
        }
        {
            std::ofstream f(base + std::to_string(rank), std::ios::binary);
            f.write(reinterpret_cast<char*>(&self), sizeof(self));
        }
        barrier(base, rank, "metadata" + phase);
        {
            std::ifstream f(base + std::to_string(1 - rank), std::ios::binary);
            if (!f.read(reinterpret_cast<char*>(&peer), sizeof(peer)))
                throw std::runtime_error("metadata read");
        }
        Wire w[2];
        w[rank] = self;
        w[1 - rank] = peer;
        std::vector<int32_t> qpns, lids;
        for (int dst = 0; dst < 2; ++dst)
            for (int c = 0; c < CHANNELS; ++c) {
                qpns.push_back(w[dst].qp[rank * CHANNELS + c]);
                lids.push_back(w[dst].lid[rank * CHANNELS + c]);
            }
        if (t->connectPeers(rank, t->isRoce(), {w[0].addr, w[1].addr},
                            {w[0].key, w[1].key}, qpns, lids,
                            {w[0].subnet, w[1].subnet},
                            {w[0].iface, w[1].iface}, {1, 1}))
            throw std::runtime_error("connect failed");
    };
    exchange_connect("initial");
    IbgdaContext ctx{static_cast<mlx5gda_qp_devctx*>(t->qpDevCtxsPtr()),
                     static_cast<uint64_t*>(t->raddrsPtr()),
                     static_cast<uint32_t*>(t->rkeysPtr()), nullptr, nullptr};
    Result* result = nullptr;
    check(cudaMalloc(reinterpret_cast<void**>(&result), sizeof(Result)));
    const Case cases[] = {{"write-1", 1, 16, 1, 0},
                          {"write-7", 7, 16, 1, 0},
                          {"write-63", 63, 16, 1, 0},
                          {"write-64", 64, 16, 1, 0},
                          {"write-257", 257, 16, 1, 0},
                          {"write-4k", 4096, 16, 1, 0},
                          {"write-1m", 1048576, 4, 1, 0},
                          {"burst-64", 4096, 256, 64, 0},
                          {"contended-8cta", 64, 256, 1, 2},
                          {"ring-wrap", 4, 16390, 1, 0},
                          {"counter-wrap", 4, 65540, 1, 0},
                          {"atomic-add32", 4, 1024, 1, 1},
                          {"atomic-negative32", 4, 1024, 1, 4},
                          {"gpu-publish-ack", 64, 1000, 1, 3},
                          {"multi-qp-4", 64, 512, 1, 5},
                          {"recreate-write", 4096, 64, 8, 0}};
    int epoch = 0;
    for (auto c : cases) {
        if (std::string(c.name) == "recreate-write") {
            barrier(base, rank, "before-recreate");
            if (t->recreateQueuePairs(nullptr))
                throw std::runtime_error("recreate failed");
            exchange_connect("recreated");
        }
        check(cudaMemset(buf, 0xa5, TOTAL));
        if (c.mode == 1 || c.mode == 4) check(cudaMemset(buf + REGION, 0, 4));
        if (c.mode == 3) {
            check(cudaMemset(buf + TOTAL - 16, 0, 4));
            check(cudaMemset(buf + TOTAL - 8, 0, 4));
        }
        check(cudaDeviceSynchronize());
        barrier(base, rank, "ready" + std::to_string(epoch));
        transfer<<<(c.mode == 2 || c.mode == 5) ? 8 : 1, 1>>>(
            ctx, buf, peer.addr, rank, epoch, c, result);
        check(cudaGetLastError());
        check(cudaDeviceSynchronize());
        if (c.mode == 2 || c.mode == 5) {
            finish<<<1, 1>>>(ctx, rank, c.mode == 5 ? CHANNELS : 1, result);
            check(cudaGetLastError());
            check(cudaDeviceSynchronize());
        }
        Result r{};
        check(cudaMemcpy(&r, result, sizeof(r), cudaMemcpyDeviceToHost));
        std::cout << "rank=" << rank << " case=" << c.name
                  << " status=" << r.status << " expect=" << r.expected
                  << " cq=" << r.counter << " owner=" << r.owner << std::endl;
        if (r.status != 1) throw std::runtime_error("CQ failure");
        barrier(base, rank, "completed" + std::to_string(epoch));
        std::vector<unsigned char> received(REGION);
        check(cudaMemcpy(received.data(), buf + REGION, REGION,
                         cudaMemcpyDeviceToHost));
        bool atomic = c.mode == 1 || c.mode == 4;
        size_t used = atomic        ? 4
                      : c.mode == 3 ? c.bytes
                                    : size_t(c.bytes) * c.count;
        for (size_t i = 0; i < REGION; ++i) {
            unsigned char expected = 0xa5;
            unsigned atomic_value =
                c.mode == 4 ? 0u - unsigned(c.count) : unsigned(c.count);
            if (i < used)
                expected =
                    atomic ? static_cast<unsigned char>(
                                 (atomic_value >> (8 * i)) & 255)
                           : pattern(1 - rank, epoch,
                                     c.mode == 3 ? c.count - 1 : i / c.bytes,
                                     i % c.bytes);
            if (c.mode == 3 && ((i >= REGION - 16 && i < REGION - 12) ||
                                (i >= REGION - 8 && i < REGION - 4)))
                expected = static_cast<unsigned char>(unsigned(c.count) >>
                                                      (8 * (i % 8)));
            if (received[i] != expected) {
                std::cerr << "byte=" << i << " actual=" << unsigned(received[i])
                          << " expected=" << unsigned(expected) << std::endl;
                throw std::runtime_error("payload/guard mismatch");
            }
        }
        std::cout << "PASS " << c.name << " rank=" << rank
                  << " operations=" << c.count << std::endl;
        barrier(base, rank, "verified" + std::to_string(epoch));
        ++epoch;
    }
    t.reset();
    check(cudaFree(result));
    check(cudaFree(buf));
    std::cout << "SUITE PASS rank=" << rank << std::endl;
    return 0;
} catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << std::endl;
    return 1;
}
