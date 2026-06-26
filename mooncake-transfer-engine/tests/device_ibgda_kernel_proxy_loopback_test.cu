// MACA IBGDA proxy evidence test.
//
// This is intentionally not a production transport. It proves the fallback
// protocol shape for MACA where device kernels cannot write mlx5 UAR/MMIO:
// a replay-safe device-visible request ring is filled by a kernel, consumed by
// a persistent host proxy, and completed through host-side mlx5 doorbells.
//
// The ring uses monotonic producer/completion counters so a captured graph can
// replay the same kernel arguments without observing stale completion state.

#include <arpa/inet.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cuda_alike.h"
#include "transport/device/device_ops.cuh"
#include "transport/device/ibgda/mlx5gda.h"

#if defined(USE_MACA)
using McGraph = mcGraph_t;
using McGraphExec = mcGraphExec_t;
#define MC_STREAM_CAPTURE_MODE_GLOBAL mcStreamCaptureModeGlobal
#define MC_STREAM_BEGIN_CAPTURE mcStreamBeginCapture
#define MC_STREAM_END_CAPTURE mcStreamEndCapture
#define MC_GRAPH_INSTANTIATE mcGraphInstantiate
#define MC_GRAPH_LAUNCH mcGraphLaunch
#define MC_GRAPH_EXEC_DESTROY mcGraphExecDestroy
#define MC_GRAPH_DESTROY mcGraphDestroy
#else
using McGraph = cudaGraph_t;
using McGraphExec = cudaGraphExec_t;
#define MC_STREAM_CAPTURE_MODE_GLOBAL cudaStreamCaptureModeGlobal
#define MC_STREAM_BEGIN_CAPTURE cudaStreamBeginCapture
#define MC_STREAM_END_CAPTURE cudaStreamEndCapture
#define MC_GRAPH_INSTANTIATE cudaGraphInstantiate
#define MC_GRAPH_LAUNCH cudaGraphLaunch
#define MC_GRAPH_EXEC_DESTROY cudaGraphExecDestroy
#define MC_GRAPH_DESTROY cudaGraphDestroy
#endif

constexpr int kMemheapMaxAllocations = 1024;

struct memheap_allocation {
    size_t offset;
    size_t size;
    bool used;
};

struct memheap {
    size_t size;
    pthread_mutex_t lock;
    size_t allocated;
    memheap_allocation allocs[kMemheapMaxAllocations];
    int alloc_count;
};

DEFINE_string(device, "mlx5_10", "RDMA device name");
DEFINE_int32(rank, 0, "Rank of this process");
DEFINE_int32(world_size, 1, "1 for self-loopback, 2 for rank0->rank1");
DEFINE_int32(gpu_id, 0, "CUDA-like device id");
DEFINE_int32(wqe, 16, "Number of WQEBBs");
DEFINE_int32(timeout_ms, 1000, "Completion timeout in milliseconds");
DEFINE_string(ipc_dir, "/tmp/mooncake-ibgda-proxy",
              "Directory for 2-rank metadata exchange");
DEFINE_int32(replays, 1,
             "Number of fixed-parameter kernel submissions to test replay-safe epochs");
DEFINE_int32(ring_size, 64, "Power-of-two proxy ring slots");
DEFINE_int32(requests_per_replay, 4, "RDMA WRITE requests submitted per replay");
DEFINE_int32(proxy_batch, 4, "Maximum WQEs posted per host proxy doorbell");
DEFINE_bool(use_graph, false,
            "Capture the submit kernel once and replay it with cudaGraphLaunch");

namespace {

constexpr size_t kCtrlBufSize = 64ULL * 1024 * 1024;
constexpr size_t kPageSize = 4096;
constexpr uint64_t kPayloadBase = 0x3141592653589793ULL;

enum ProxySlotState : uint32_t {
    kSlotFree = 0,
    kSlotPosted = 1,
    kSlotDone = 2,
    kSlotError = 3,
};

struct alignas(64) ProxyRingSlot {
    uint32_t state;
    uint32_t opcode;
    uint32_t bytes;
    uint32_t lkey;
    uint32_t rkey;
    uint32_t reserved;
    uint64_t src;
    uint64_t dst;
    uint64_t seq;
};

struct alignas(64) ProxyRing {
    uint32_t size_mask;
    uint32_t reserved0[15];

    uint64_t producer_tail;
    uint64_t reserved1[7];

    uint64_t consumer_head;
    uint64_t doorbell_tail;
    uint64_t reserved2[6];

    uint64_t completion_head;
    uint64_t error_head;
    uint64_t reserved3[6];

    ProxyRingSlot slots[];
};

struct RankMeta {
    uint32_t qpn;
    uint32_t rkey;
    uint64_t addr;
    uint16_t lid;
    uint8_t gid[16];
};

memheap* createMemheap(size_t size) {
    auto* heap = static_cast<memheap*>(std::malloc(sizeof(memheap)));
    if (!heap) return nullptr;
    heap->size = size;
    heap->allocated = 0;
    heap->alloc_count = 0;
    pthread_mutex_init(&heap->lock, nullptr);
    return heap;
}

void destroyMemheap(memheap* heap) {
    if (!heap) return;
    pthread_mutex_destroy(&heap->lock);
    std::free(heap);
}

uint16_t bswap16(uint16_t v) { return __builtin_bswap16(v); }
uint32_t bswap32(uint32_t v) { return __builtin_bswap32(v); }
uint64_t bswap64(uint64_t v) { return __builtin_bswap64(v); }

void check(bool cond, const char* what) {
    if (!cond) throw std::runtime_error(what);
}

void checkCuda(cudaError_t err, const char* what) {
    if (err != cudaSuccess) {
        throw std::runtime_error(std::string(what) + ": " +
                                 cudaGetErrorString(err));
    }
}

int findBestGidIndex(ibv_context* ctx, uint8_t port,
                     const ibv_port_attr& port_attr) {
    for (int i = 0; i < port_attr.gid_tbl_len; ++i) {
        ibv_gid_entry entry;
        if (ibv_query_gid_ex(ctx, port, i, &entry, 0)) continue;

        auto* a = reinterpret_cast<const struct in6_addr*>(entry.gid.raw);
        bool v4mapped = ((a->s6_addr32[0] | a->s6_addr32[1]) == 0 &&
                         a->s6_addr32[2] == htonl(0x0000ffff));
        if (entry.gid_type == IBV_GID_TYPE_ROCE_V2 && v4mapped) return i;
        if (entry.gid_type == IBV_GID_TYPE_IB) return i;
    }
    return -1;
}

ibv_context* openDevice(const std::string& name) {
    int num_devices = 0;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    check(dev_list != nullptr, "ibv_get_device_list failed");

    ibv_device* found = nullptr;
    for (int i = 0; i < num_devices; ++i) {
        if (name == ibv_get_device_name(dev_list[i])) {
            found = dev_list[i];
            break;
        }
    }
    check(found != nullptr, "requested RDMA device not found");
    ibv_context* ctx = ibv_open_device(found);
    ibv_free_device_list(dev_list);
    check(ctx != nullptr, "ibv_open_device failed");
    return ctx;
}

void postHostDoorbell(mlx5gda_qp* qp, mlx5gda_wq_dbr* dbr,
                      mlx5gda_wqebb* wq, uint32_t wq_head) {
    dbr->send_counter = bswap32(wq_head);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    *reinterpret_cast<volatile uint64_t*>(qp->uar->reg_addr) =
        wq[(wq_head - 1) & (qp->num_wqebb - 1)].qwords[0];
}

struct CqPoller {
    mlx5_cqe64* cq;
    mlx5gda_cq_dbr* dbr;
    uint32_t cqe_mask;
    uint64_t ci = 0;
    uint16_t completed_wqe = 0;

    void updateConsumerIndex() {
        auto* ci_be = reinterpret_cast<volatile uint32_t*>(dbr);
        *ci_be = bswap32(static_cast<uint32_t>(ci));
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    bool pollUntil(uint32_t expect_wq_head,
                   std::chrono::steady_clock::time_point deadline) {
        uint16_t expect16 = static_cast<uint16_t>(expect_wq_head);
        while (std::chrono::steady_clock::now() < deadline) {
            mlx5_cqe64* cqe = &cq[ci & cqe_mask];
            uint8_t op_own = cqe->op_own;
            uint8_t opcode = op_own >> 4;
            uint8_t owner = op_own & 0x1;
            uint8_t expected_owner = static_cast<uint8_t>((ci >> 0) /
                                                          (cqe_mask + 1)) &
                                     0x1;
            if (opcode == 0xD && owner == expected_owner) return false;
            if ((opcode == 0x0 || opcode == 0xF) &&
                owner == expected_owner) {
                completed_wqe =
                    static_cast<uint16_t>(bswap16(cqe->wqe_counter) + 1);
                ++ci;
                updateConsumerIndex();
                if (static_cast<int16_t>(completed_wqe - expect16) >= 0)
                    return true;
                continue;
            }

            // Collapsed CQ mode can keep updating a CQE that is not the next
            // owner-bit entry. The evidence test scans for a CQE whose WQE
            // counter covers the requested doorbell batch.
            for (uint32_t i = 0; i <= cqe_mask; ++i) {
                mlx5_cqe64* collapsed = &cq[i];
                uint8_t collapsed_opcode = collapsed->op_own >> 4;
                if (collapsed_opcode == 0xD) return false;
                if (collapsed_opcode != 0x0 && collapsed_opcode != 0xF)
                    continue;
                uint16_t collapsed_completed = static_cast<uint16_t>(
                    bswap16(collapsed->wqe_counter) + 1);
                if (static_cast<int16_t>(collapsed_completed - expect16) >=
                    0) {
                    completed_wqe = collapsed_completed;
                    return true;
                }
            }

            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        return false;
    }
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

RankMeta readMeta(int rank, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream ifs(metaPath(rank), std::ios::binary);
        if (ifs) {
            RankMeta meta{};
            ifs.read(reinterpret_cast<char*>(&meta), sizeof(meta));
            if (ifs.gcount() == static_cast<std::streamsize>(sizeof(meta))) {
                return meta;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("timeout waiting for peer metadata");
}

void writeDone() {
    std::ofstream ofs(donePath(), std::ios::trunc);
    check(static_cast<bool>(ofs), "open done for write failed");
    ofs << "done\n";
}

void waitDone(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream ifs(donePath());
        if (ifs) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("timeout waiting for rank0 done");
}

void proxyDrainUntil(ProxyRing* ring, mlx5gda_qp* qp, mlx5gda_wqebb* wq,
                     mlx5gda_wq_dbr* dbr, CqPoller* cq_poller,
                     uint64_t target_tail, int timeout_ms, int max_batch) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    auto* producer_tail =
        reinterpret_cast<std::atomic<uint64_t>*>(&ring->producer_tail);
    auto* completion_head =
        reinterpret_cast<std::atomic<uint64_t>*>(&ring->completion_head);

    while (completion_head->load(std::memory_order_acquire) < target_tail) {
        uint64_t available = producer_tail->load(std::memory_order_acquire);
        uint64_t head = ring->consumer_head;
        if (head == available) {
            if (std::chrono::steady_clock::now() >= deadline) {
                ring->error_head = head;
                throw std::runtime_error("timeout waiting for posted requests");
            }
            std::this_thread::sleep_for(std::chrono::microseconds(20));
            continue;
        }

        uint64_t n = std::min<uint64_t>(available - head, max_batch);
        n = std::min<uint64_t>(n, qp->num_wqebb);
        for (uint64_t i = 0; i < n; ++i) {
            uint64_t seq = head + i;
            auto* slot = &ring->slots[seq & ring->size_mask];
            if (slot->state != kSlotPosted || slot->seq != seq) {
                ring->error_head = seq;
                throw std::runtime_error("proxy observed an invalid ring slot");
            }
            auto wqe_idx =
                static_cast<uint32_t>(seq & (qp->num_wqebb - 1));
            auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(wq + wqe_idx);
            std::memset(wqe, 0, sizeof(*wqe));
            wqe->ctrl.qpn_ds = bswap32((qp->qpn << 8) | 3);
            wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
            wqe->ctrl.opmod_idx_opcode =
                bswap32((static_cast<uint32_t>(seq) << 8) |
                        MLX5_OPCODE_RDMA_WRITE);
            wqe->raddr.raddr = bswap64(slot->dst);
            wqe->raddr.rkey = bswap32(slot->rkey);
            wqe->data.byte_count = bswap32(slot->bytes);
            wqe->data.lkey = bswap32(slot->lkey);
            wqe->data.addr = bswap64(slot->src);
        }

        uint32_t wq_head = static_cast<uint32_t>(head + n);
        postHostDoorbell(qp, dbr, wq, wq_head);
        if (!cq_poller->pollUntil(wq_head, deadline)) {
            ring->error_head = head;
            throw std::runtime_error("RDMA WRITE batch did not complete");
        }
        for (uint64_t i = 0; i < n; ++i) {
            auto* slot = &ring->slots[(head + i) & ring->size_mask];
            slot->state = kSlotDone;
        }
        ring->consumer_head = head + n;
        ring->doorbell_tail = head + n;
        completion_head->store(head + n, std::memory_order_release);
    }
}

__global__ void submitProxyRingBatchKernel(ProxyRing* ring, uint64_t src_base,
                                           uint64_t dst_base, uint32_t lkey,
                                           uint32_t rkey, uint32_t requests,
                                           uint64_t* out) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;

    uint64_t tail = ring->producer_tail;
    uint64_t needed_tail = tail + requests;
    uint32_t spins = 0;

    uint32_t submitted = 0;
    while (submitted < requests) {
        uint64_t completion =
            *const_cast<volatile uint64_t*>(&ring->completion_head);
        uint64_t inflight = tail - completion;
        uint64_t capacity = static_cast<uint64_t>(ring->size_mask + 1);
        while (inflight >= capacity) {
            if (++spins > 100000000u) {
                out[0] = kSlotError;
                out[1] = spins;
                out[2] = tail;
                return;
            }
            __threadfence_system();
            completion = *const_cast<volatile uint64_t*>(
                &ring->completion_head);
            inflight = tail - completion;
        }

        uint32_t n = static_cast<uint32_t>(
            min(static_cast<uint64_t>(requests - submitted),
                capacity - inflight));
        for (uint32_t i = 0; i < n; ++i) {
            uint64_t seq = tail + i;
            auto* slot = &ring->slots[seq & ring->size_mask];
            slot->state = kSlotFree;
            slot->opcode = MLX5_OPCODE_RDMA_WRITE;
            slot->bytes = sizeof(uint64_t);
            slot->lkey = lkey;
            slot->rkey = rkey;
            slot->src = src_base;
            slot->dst = dst_base +
                        static_cast<uint64_t>(submitted + i) *
                            sizeof(uint64_t);
            slot->seq = seq;
            mooncake::device::mc_st_release_u32(&slot->state, kSlotPosted);
        }
        __threadfence_system();
        tail += n;
        ring->producer_tail = tail;
        __threadfence_system();
        submitted += n;
    }

    while (*const_cast<volatile uint64_t*>(&ring->completion_head) <
           needed_tail) {
        if (++spins > 100000000u) {
            out[0] = kSlotError;
            out[1] = spins;
            return;
        }
        __threadfence_system();
    }
    out[0] = kSlotDone;
    out[1] = spins;
    out[2] = needed_tail;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    ibv_context* ctx = nullptr;
    ibv_pd* pd = nullptr;
    mlx5dv_devx_umem* ctrl_umem = nullptr;
    memheap* heap = nullptr;
    mlx5gda_qp* qp = nullptr;
    ibv_mr* data_mr = nullptr;
    void* ctrl_buf = nullptr;
    void* gpu_buf = nullptr;
    uint64_t* out = nullptr;
    ProxyRing* ring_host = nullptr;
    ProxyRing* ring_dev = nullptr;
    int dmabuf_fd = -1;

    try {
        check(FLAGS_world_size == 1 || FLAGS_world_size == 2,
              "--world_size must be 1 or 2");
        check(FLAGS_rank >= 0 && FLAGS_rank < FLAGS_world_size,
              "invalid --rank");
        check(FLAGS_replays > 0, "--replays must be positive");
        check(FLAGS_ring_size > 0 &&
                  (FLAGS_ring_size & (FLAGS_ring_size - 1)) == 0,
              "--ring_size must be a power of two");
        check(FLAGS_requests_per_replay > 0,
              "--requests_per_replay must be positive");
        check(FLAGS_requests_per_replay <= FLAGS_wqe,
              "--requests_per_replay must fit in the QP WQE ring");
        check(FLAGS_proxy_batch > 0 && FLAGS_proxy_batch <= FLAGS_wqe,
              "--proxy_batch must fit in the QP WQE ring");
        checkCuda(cudaSetDevice(FLAGS_gpu_id), "cudaSetDevice");

        ctx = openDevice(FLAGS_device);
        pd = ibv_alloc_pd(ctx);
        check(pd != nullptr, "ibv_alloc_pd failed");

        mlx5dv_obj dv_obj{};
        mlx5dv_pd mpd{};
        dv_obj.pd.in = pd;
        dv_obj.pd.out = &mpd;
        check(mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD) == 0,
              "mlx5dv_init_obj PD failed");

        int ret = posix_memalign(&ctrl_buf, kPageSize, kCtrlBufSize);
        check(ret == 0, "posix_memalign ctrl_buf failed");
        std::memset(ctrl_buf, 0, kCtrlBufSize);
        checkCuda(cudaHostRegister(ctrl_buf, kCtrlBufSize,
                                   cudaHostRegisterPortable |
                                       cudaHostRegisterMapped),
                  "cudaHostRegister ctrl_buf");
        ctrl_umem = mlx5dv_devx_umem_reg(ctx, ctrl_buf, kCtrlBufSize,
                                         IBV_ACCESS_LOCAL_WRITE);
        check(ctrl_umem != nullptr, "mlx5dv_devx_umem_reg ctrl_buf failed");
        heap = createMemheap(kCtrlBufSize);
        check(heap != nullptr, "createMemheap failed");

        qp = mlx5gda_create_rc_qp(mpd, ctrl_buf, ctrl_umem, heap, pd,
                                  FLAGS_wqe, 1, nullptr);
        check(qp != nullptr, "mlx5gda_create_rc_qp failed");
        check(mlx5gda_modify_rc_qp_rst2init(qp, 0) == 0, "rst2init failed");

        ibv_port_attr port_attr{};
        check(ibv_query_port(ctx, 1, &port_attr) == 0, "ibv_query_port failed");
        int gid_index = findBestGidIndex(ctx, 1, port_attr);
        check(gid_index >= 0, "No suitable GID found");
        ibv_gid gid{};
        check(ibv_query_gid(ctx, 1, gid_index, &gid) == 0,
              "ibv_query_gid failed");

        ibv_ah_attr ah_attr{};
        if (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
            ah_attr.is_global = 1;
            ah_attr.grh.dgid = gid;
            ah_attr.grh.sgid_index = gid_index;
            ah_attr.grh.hop_limit = 255;
            ah_attr.port_num = 1;
            ah_attr.dlid = port_attr.lid | 0xC000;
        } else {
            ah_attr.dlid = port_attr.lid;
            ah_attr.port_num = 1;
        }
        size_t payload_words = 1 + static_cast<size_t>(FLAGS_requests_per_replay);
        size_t payload_bytes = payload_words * sizeof(uint64_t);
        checkCuda(cudaMalloc(&gpu_buf, payload_bytes), "cudaMalloc gpu_buf");
        std::vector<uint64_t> init(payload_words, 0);
        init[0] = kPayloadBase + static_cast<uint64_t>(FLAGS_rank);
        checkCuda(cudaMemcpy(gpu_buf, init.data(), payload_bytes, cudaMemcpyHostToDevice),
                  "cudaMemcpy init");

        checkCuda(cuMemGetHandleForAddressRange(
                      &dmabuf_fd, reinterpret_cast<CUdeviceptr>(gpu_buf),
                      payload_bytes, CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0),
                  "cuMemGetHandleForAddressRange dmabuf");
        data_mr = ibv_reg_dmabuf_mr(
            pd, 0, payload_bytes, reinterpret_cast<uint64_t>(gpu_buf),
            dmabuf_fd,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE);
        check(data_mr != nullptr, "ibv_reg_dmabuf_mr gpu_buf failed");

        RankMeta local_meta{};
        local_meta.qpn = qp->qpn;
        local_meta.rkey = data_mr->rkey;
        local_meta.addr = reinterpret_cast<uint64_t>(gpu_buf);
        local_meta.lid = port_attr.lid;
        std::memcpy(local_meta.gid, gid.raw, sizeof(local_meta.gid));

        RankMeta remote_meta = local_meta;
        if (FLAGS_world_size == 2) {
            writeMeta(FLAGS_rank, local_meta);
            remote_meta = readMeta(1 - FLAGS_rank, FLAGS_timeout_ms);
            std::memcpy(ah_attr.grh.dgid.raw, remote_meta.gid,
                        sizeof(remote_meta.gid));
            if (port_attr.link_layer != IBV_LINK_LAYER_ETHERNET) {
                ah_attr.dlid = remote_meta.lid;
            }
        }

        check(mlx5gda_modify_rc_qp_init2rtr(qp, ah_attr, remote_meta.qpn,
                                            IBV_MTU_4096) == 0,
              "init2rtr failed");
        check(mlx5gda_modify_rc_qp_rtr2rts(qp) == 0, "rtr2rts failed");

        size_t ring_bytes = sizeof(ProxyRing) +
                            static_cast<size_t>(FLAGS_ring_size) *
                                sizeof(ProxyRingSlot);
        checkCuda(cudaHostAlloc(reinterpret_cast<void**>(&ring_host),
                                ring_bytes, cudaHostAllocMapped),
                  "cudaHostAlloc ring");
        std::memset(ring_host, 0, ring_bytes);
        ring_host->size_mask = static_cast<uint32_t>(FLAGS_ring_size - 1);
        checkCuda(cudaHostGetDevicePointer(reinterpret_cast<void**>(&ring_dev),
                                           ring_host, 0),
                  "cudaHostGetDevicePointer ring");
        checkCuda(cudaMalloc(&out, 3 * sizeof(uint64_t)), "cudaMalloc out");
        checkCuda(cudaMemset(out, 0, 3 * sizeof(uint64_t)), "cudaMemset out");

        auto* wq = reinterpret_cast<mlx5gda_wqebb*>(
            static_cast<char*>(ctrl_buf) + qp->wq_offset);
        auto* dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
            static_cast<char*>(ctrl_buf) + qp->dbr_offset);
        auto* cq = reinterpret_cast<mlx5_cqe64*>(
            static_cast<char*>(ctrl_buf) + qp->send_cq->cq_offset);
        auto* cq_dbr = reinterpret_cast<mlx5gda_cq_dbr*>(
            static_cast<char*>(ctrl_buf) + qp->send_cq->dbr_offset);
        CqPoller cq_poller{cq, cq_dbr, qp->send_cq->cqe - 1};

        uint64_t last_spins = 0;
        if (FLAGS_world_size == 1 || FLAGS_rank == 0) {
            uint64_t src_addr = reinterpret_cast<uint64_t>(gpu_buf);
            uint64_t dst_addr =
                (FLAGS_world_size == 1)
                    ? reinterpret_cast<uint64_t>(static_cast<char*>(gpu_buf) +
                                                 sizeof(uint64_t))
                    : remote_meta.addr + sizeof(uint64_t);
            uint32_t remote_rkey =
                (FLAGS_world_size == 1) ? data_mr->rkey : remote_meta.rkey;
            McGraph graph = nullptr;
            McGraphExec graph_exec = nullptr;
            cudaStream_t graph_stream = nullptr;
            if (FLAGS_use_graph) {
                checkCuda(cudaStreamCreate(&graph_stream),
                          "cudaStreamCreate graph_stream");
                checkCuda(MC_STREAM_BEGIN_CAPTURE(graph_stream,
                                                 MC_STREAM_CAPTURE_MODE_GLOBAL),
                          "cudaStreamBeginCapture");
                submitProxyRingBatchKernel<<<1, 1, 0, graph_stream>>>(
                    ring_dev, src_addr, dst_addr, data_mr->lkey, remote_rkey,
                    static_cast<uint32_t>(FLAGS_requests_per_replay), out);
                checkCuda(cudaGetLastError(),
                          "submitProxyRingBatchKernel graph capture launch");
                checkCuda(MC_STREAM_END_CAPTURE(graph_stream, &graph),
                          "cudaStreamEndCapture");
                checkCuda(MC_GRAPH_INSTANTIATE(&graph_exec, graph, nullptr,
                                               nullptr, 0),
                          "cudaGraphInstantiate");
            }

            for (int replay = 1; replay <= FLAGS_replays; ++replay) {
                uint64_t target_tail =
                    ring_host->completion_head + FLAGS_requests_per_replay;
                std::thread proxy([&] {
                    proxyDrainUntil(ring_host, qp, wq, dbr, &cq_poller,
                                    target_tail, FLAGS_timeout_ms,
                                    FLAGS_proxy_batch);
                });

                if (FLAGS_use_graph) {
                    checkCuda(MC_GRAPH_LAUNCH(graph_exec, graph_stream),
                              "cudaGraphLaunch");
                    checkCuda(cudaStreamSynchronize(graph_stream),
                              "cudaGraph replay sync");
                } else {
                    submitProxyRingBatchKernel<<<1, 1>>>(
                        ring_dev, src_addr, dst_addr, data_mr->lkey,
                        remote_rkey,
                        static_cast<uint32_t>(FLAGS_requests_per_replay), out);
                    checkCuda(cudaGetLastError(),
                              "submitProxyRingBatchKernel launch");
                    checkCuda(cudaDeviceSynchronize(),
                              "submitProxyRingBatchKernel sync");
                }
                proxy.join();

                uint64_t host_out[3] = {};
                checkCuda(cudaMemcpy(host_out, out, sizeof(host_out),
                                     cudaMemcpyDeviceToHost),
                          "cudaMemcpy out");
                check(host_out[0] == kSlotDone,
                      "proxy ring batch did not complete");
                check(host_out[2] == target_tail,
                      "kernel observed unexpected completion tail");
                last_spins = host_out[1];
            }
            if (graph_exec) MC_GRAPH_EXEC_DESTROY(graph_exec);
            if (graph) MC_GRAPH_DESTROY(graph);
            if (graph_stream) cudaStreamDestroy(graph_stream);
        }

        if (FLAGS_world_size == 1) {
            std::vector<uint64_t> result(payload_words, 0);
            checkCuda(cudaMemcpy(result.data(), gpu_buf, payload_bytes,
                                 cudaMemcpyDeviceToHost),
                      "cudaMemcpy result");
            for (int i = 0; i < FLAGS_requests_per_replay; ++i) {
                check(result[1 + i] == init[0], "RDMA loopback data mismatch");
            }
        } else if (FLAGS_rank == 0) {
            writeDone();
        } else {
            waitDone(FLAGS_timeout_ms);
            std::vector<uint64_t> result(payload_words, 0);
            checkCuda(cudaMemcpy(result.data(), gpu_buf, payload_bytes,
                                 cudaMemcpyDeviceToHost),
                      "cudaMemcpy target result");
            for (int i = 0; i < FLAGS_requests_per_replay; ++i) {
                check(result[1 + i] == kPayloadBase,
                      "2-rank RDMA target data mismatch");
            }
        }

        std::cout << "device_ibgda_kernel_proxy_loopback_test passed"
                  << " device=" << FLAGS_device << " gid_index=" << gid_index
                  << " rank=" << FLAGS_rank << "/" << FLAGS_world_size
                  << " qpn=" << qp->qpn << " lkey=0x" << std::hex
                  << data_mr->lkey << " rkey=0x" << data_mr->rkey << std::dec
                  << " replays=" << FLAGS_replays << " spins=" << last_spins
                  << "\n";

        if (out) cudaFree(out);
        if (ring_host) cudaFreeHost(ring_host);
        if (data_mr) ibv_dereg_mr(data_mr);
        if (dmabuf_fd >= 0) close(dmabuf_fd);
        if (gpu_buf) cudaFree(gpu_buf);
        if (qp) mlx5gda_destroy_qp(heap, qp);
        if (heap) destroyMemheap(heap);
        if (ctrl_umem) mlx5dv_devx_umem_dereg(ctrl_umem);
        if (ctrl_buf) {
            cudaHostUnregister(ctrl_buf);
            free(ctrl_buf);
        }
        if (pd) ibv_dealloc_pd(pd);
        if (ctx) ibv_close_device(ctx);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "device_ibgda_kernel_proxy_loopback_test failed: "
                  << e.what() << "\n";
        if (out) cudaFree(out);
        if (ring_host) cudaFreeHost(ring_host);
        if (data_mr) ibv_dereg_mr(data_mr);
        if (dmabuf_fd >= 0) close(dmabuf_fd);
        if (gpu_buf) cudaFree(gpu_buf);
        if (qp) mlx5gda_destroy_qp(heap, qp);
        if (heap) destroyMemheap(heap);
        if (ctrl_umem) mlx5dv_devx_umem_dereg(ctrl_umem);
        if (ctrl_buf) {
            cudaHostUnregister(ctrl_buf);
            free(ctrl_buf);
        }
        if (pd) ibv_dealloc_pd(pd);
        if (ctx) ibv_close_device(ctx);
        return 1;
    }
}
