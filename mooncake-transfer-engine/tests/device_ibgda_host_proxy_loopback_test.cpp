#include <arpa/inet.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cuda_alike.h"
#include "transport/device/ibgda/memheap.h"
#include "transport/device/ibgda/mlx5gda.h"

DEFINE_string(device, "mlx5_10", "RDMA device name");
DEFINE_int32(gpu_id, 0, "CUDA-like device id");
DEFINE_int32(wqe, 16, "Number of WQEBBs");
DEFINE_int32(timeout_ms, 1000, "Completion timeout in milliseconds");

namespace {

constexpr size_t kCtrlBufSize = 64ULL * 1024 * 1024;
constexpr size_t kPageSize = 4096;

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
                      mlx5gda_wqebb* wq, uint16_t wq_head) {
    dbr->send_counter = bswap32(wq_head);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    *reinterpret_cast<volatile uint64_t*>(qp->uar->reg_addr) = wq[0].qwords[0];
}

bool pollCompletion(mlx5_cqe64* cq, uint16_t expect, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        uint8_t opcode = cq->op_own >> 4;
        if (opcode == 0x0 || opcode == 0xF) {
            uint16_t wqe_counter = bswap16(cq->wqe_counter);
            if (static_cast<int16_t>(wqe_counter + 1 - expect) >= 0) {
                return true;
            }
        }
        if (opcode == 0xD) {
            std::cerr << "requester error CQE syndrome=0x" << std::hex
                      << static_cast<uint64_t>(cq->timestamp >> 56) << std::dec
                      << "\n";
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
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
    int dmabuf_fd = -1;

    try {
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
        heap = memheap_create(kCtrlBufSize);
        check(heap != nullptr, "memheap_create failed");

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
        check(mlx5gda_modify_rc_qp_init2rtr(qp, ah_attr, qp->qpn,
                                            IBV_MTU_4096) == 0,
              "init2rtr self failed");
        check(mlx5gda_modify_rc_qp_rtr2rts(qp) == 0, "rtr2rts failed");

        checkCuda(cudaMalloc(&gpu_buf, 2 * sizeof(uint64_t)),
                  "cudaMalloc gpu_buf");
        const uint64_t init[2] = {0x123456789abcdef0ULL, 0};
        checkCuda(cudaMemcpy(gpu_buf, init, sizeof(init), cudaMemcpyHostToDevice),
                  "cudaMemcpy init");

        checkCuda(cuMemGetHandleForAddressRange(
                      &dmabuf_fd, reinterpret_cast<CUdeviceptr>(gpu_buf),
                      2 * sizeof(uint64_t),
                      CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0),
                  "cuMemGetHandleForAddressRange dmabuf");
        data_mr = ibv_reg_dmabuf_mr(
            pd, 0, 2 * sizeof(uint64_t), reinterpret_cast<uint64_t>(gpu_buf),
            dmabuf_fd,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE);
        check(data_mr != nullptr, "ibv_reg_dmabuf_mr gpu_buf failed");

        auto* wq = reinterpret_cast<mlx5gda_wqebb*>(
            static_cast<char*>(ctrl_buf) + qp->wq_offset);
        auto* dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
            static_cast<char*>(ctrl_buf) + qp->dbr_offset);
        auto* cq = reinterpret_cast<mlx5_cqe64*>(
            static_cast<char*>(ctrl_buf) + qp->send_cq->cq_offset);

        auto* wqe = reinterpret_cast<mlx5gda_rdma_write_wqe*>(wq);
        std::memset(wqe, 0, sizeof(*wqe));
        wqe->ctrl.qpn_ds = bswap32((qp->qpn << 8) | 3);
        wqe->ctrl.fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
        wqe->ctrl.opmod_idx_opcode = bswap32(MLX5_OPCODE_RDMA_WRITE);
        wqe->raddr.raddr =
            bswap64(reinterpret_cast<uint64_t>(static_cast<char*>(gpu_buf) +
                                               sizeof(uint64_t)));
        wqe->raddr.rkey = bswap32(data_mr->rkey);
        wqe->data.byte_count = bswap32(sizeof(uint64_t));
        wqe->data.lkey = bswap32(data_mr->lkey);
        wqe->data.addr = bswap64(reinterpret_cast<uint64_t>(gpu_buf));

        postHostDoorbell(qp, dbr, wq, 1);
        check(pollCompletion(cq, 1, FLAGS_timeout_ms),
              "RDMA WRITE did not complete");

        uint64_t result[2] = {};
        checkCuda(cudaMemcpy(result, gpu_buf, sizeof(result),
                             cudaMemcpyDeviceToHost),
                  "cudaMemcpy result");
        check(result[1] == init[0], "RDMA loopback data mismatch");

        std::cout << "device_ibgda_host_proxy_loopback_test passed"
                  << " device=" << FLAGS_device << " gid_index=" << gid_index
                  << " qpn=" << qp->qpn << " lkey=0x" << std::hex
                  << data_mr->lkey << " rkey=0x" << data_mr->rkey << std::dec
                  << "\n";

        if (data_mr) ibv_dereg_mr(data_mr);
        if (dmabuf_fd >= 0) close(dmabuf_fd);
        if (gpu_buf) cudaFree(gpu_buf);
        if (qp) mlx5gda_destroy_qp(heap, qp);
        if (heap) memheap_destroy(heap);
        if (ctrl_umem) mlx5dv_devx_umem_dereg(ctrl_umem);
        if (ctrl_buf) {
            cudaHostUnregister(ctrl_buf);
            free(ctrl_buf);
        }
        if (pd) ibv_dealloc_pd(pd);
        if (ctx) ibv_close_device(ctx);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "device_ibgda_host_proxy_loopback_test failed: "
                  << e.what() << "\n";
        if (data_mr) ibv_dereg_mr(data_mr);
        if (dmabuf_fd >= 0) close(dmabuf_fd);
        if (gpu_buf) cudaFree(gpu_buf);
        if (qp) mlx5gda_destroy_qp(heap, qp);
        if (heap) memheap_destroy(heap);
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
