#ifndef _MLX5GDA_H_
#define _MLX5GDA_H_

#include <stddef.h>
#include <stdint.h>
#include <cuda_runtime.h>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>

#define MAX_QP_COUNT 256

struct mlx5gda_cq_dbr {
    uint64_t unused;
};

struct mlx5gda_wq_dbr {
    __be32 rcv_counter;   // be, low 16 bits significant
    __be32 send_counter;  // be, low 16 bits significant
};

struct mlx5gda_wqebb {
    uint64_t qwords[8];  // 64 bytes
};

struct mlx5gda_rdma_write_wqe {
    struct mlx5_wqe_ctrl_seg ctrl;
    struct mlx5_wqe_raddr_seg raddr;
    struct mlx5_wqe_data_seg data;
};

struct mlx5gda_rdma_atomic_wqe {
    struct mlx5_wqe_ctrl_seg ctrl;
    struct mlx5_wqe_raddr_seg raddr;
    struct mlx5_wqe_atomic_seg atomic;
    struct mlx5_wqe_data_seg data;
};

struct mlx5gda_cq {
    struct mlx5dv_devx_obj *mcq;
    struct mlx5dv_devx_uar *uar;  // uar is allocated but not used
    uint32_t cqn;
    uint32_t cqe;
    size_t cq_offset;
    size_t dbr_offset;
};

struct mlx5gda_cq *mlx5gda_create_cq(void *ctrl_buf,
                                     struct mlx5dv_devx_umem *ctrl_buf_umem,
                                     struct memheap *ctrl_buf_heap,
                                     struct ibv_pd *pd, int num_cqe,
                                     cudaStream_t stream);
void mlx5gda_destroy_cq(struct memheap *ctrl_buf_heap, struct mlx5gda_cq *cq);

static const size_t MLX5GDA_BF_SIZE = 256;

struct mlx5gda_qp {
    struct mlx5dv_devx_obj *mqp;
    struct mlx5gda_cq *send_cq;
    struct mlx5dv_devx_uar *uar;

    uint8_t port_num;
    struct ibv_port_attr port_attr;

    struct ibv_pd *pd;

    uint32_t qpn;
    uint32_t num_wqebb;
    size_t wq_offset;
    size_t dbr_offset;
};

struct mlx5gda_qp_devctx {
    uint32_t qpn;         // QP number
    uint32_t wqeid_mask;  // = num_wqebb - 1
    uint32_t mutex;
    struct mlx5gda_wqebb *wq;
    struct mlx5_cqe64 *cq;
    struct mlx5gda_wq_dbr *dbr;
    char *bf;
    uint32_t bf_offset;  // toggle on every post
    uint16_t wq_head;    // next free wqeid
    uint16_t wq_tail;    // last non-completed wqeid
};

struct mlx5gda_qp *mlx5gda_create_rc_qp(struct mlx5dv_pd mpd, void *ctrl_buf,
                                        struct mlx5dv_devx_umem *ctrl_buf_umem,
                                        struct memheap *ctrl_buf_heap,
                                        struct ibv_pd *pd, int wqe,
                                        uint8_t port_num, cudaStream_t stream);
void mlx5gda_destroy_qp(struct mlx5gda_qp *qp);

int mlx5gda_modify_rc_qp_rst2init(struct mlx5gda_qp *qp, uint16_t pkey_index);
int mlx5gda_modify_rc_qp_init2rtr(struct mlx5gda_qp *qp,
                                  struct ibv_ah_attr ah_attr,
                                  uint32_t remote_qpn, enum ibv_mtu mtu);
int mlx5gda_modify_rc_qp_rtr2rts(struct mlx5gda_qp *qp);

#endif
