#include <cmath>

#include <cuda_runtime_api.h>

#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>

#include <mooncake_ibgda/memheap.h>
#include <mooncake_ibgda/mlx5gda.h>
#include <mooncake_ibgda/mlx5_ifc.h>
#include <mooncake_ibgda/mlx5_prm.h>

template <typename T>
inline T IBGDA_ILOG2(T _n) {
    return (T)ceil(log2((double)_n));
}

#define IBGDA_ILOG2_OR0(_n) (((_n) == 0) ? 0 : IBGDA_ILOG2(_n))

template <typename T>
constexpr T round_up_pow2(T n) {
    static_assert(std::is_unsigned_v<T>, "Only works with unsigned types");
    if (n <= 1) return 1;

    T pow2 = 1;
    while (pow2 < n) pow2 <<= 1;
    return pow2;
}

#define IBGDA_ROUND_UP_POW2_OR_0(_n) (((_n) == 0) ? 0 : round_up_pow2(_n))

static void print_cuda_error(const char *msg) {
    const char *err_str = cudaGetErrorString(cudaGetLastError());
    fprintf(stderr, "%s: %s\n", msg, err_str);
}

static struct mlx5dv_devx_uar *create_uar(struct ibv_context *ctx) {
    struct mlx5dv_devx_uar *uar =
        mlx5dv_devx_alloc_uar(ctx, MLX5DV_UAR_ALLOC_TYPE_BF);
    if (!uar) {
        errno = EIO;
        return NULL;
    }
    if (cudaHostRegister(uar->reg_addr, MLX5GDA_BF_SIZE * 2,
                         cudaHostRegisterPortable | cudaHostRegisterMapped |
                             cudaHostRegisterIoMemory) != cudaSuccess) {
        print_cuda_error("Failed to register MMIO memory");
        errno = EIO;
        mlx5dv_devx_free_uar(uar);
        return NULL;
    }
    return uar;
}

static void destroy_uar(struct mlx5dv_devx_uar *uar) {
    if (!uar) return;
    if (cudaHostUnregister(uar->reg_addr) != cudaSuccess) {
        print_cuda_error("Failed to unregister MMIO memory");
    }
    mlx5dv_devx_free_uar(uar);
}

struct mlx5gda_cq *mlx5gda_create_cq(void *ctrl_buf,
                                     struct mlx5dv_devx_umem *ctrl_buf_umem,
                                     struct memheap *ctrl_buf_heap,
                                     struct ibv_pd *pd, int cqe,
                                     cudaStream_t stream) {
    struct mlx5gda_cq *cq = NULL;
    struct mlx5dv_devx_uar *uar = NULL;
    uint32_t eqn = 0;
    size_t cq_offset = -1;
    size_t dbr_offset = -1;
    struct mlx5dv_devx_obj *mlx5_cq = NULL;
    uint32_t cqn = 0;

    struct ibv_context *ctx = pd->context;
    void *cq_context = NULL;

    if (cqe <= 0) {
        errno = EINVAL;
        return NULL;
    }
    uint32_t num_cqe = IBGDA_ROUND_UP_POW2_OR_0((uint32_t)cqe);

    uint8_t cmd_in[DEVX_ST_SZ_BYTES(create_cq_in)] = {0};
    uint8_t cmd_out[DEVX_ST_SZ_BYTES(create_cq_out)] = {0};

    cq_offset = memheap_aligned_alloc(ctrl_buf_heap,
                                      num_cqe * sizeof(struct mlx5_cqe64),
                                      (size_t)1 << MLX5_ADAPTER_PAGE_SHIFT);
    if (cq_offset == -1) {
        perror("Failed to allocate CQ memory");
        goto fail;
    }
    // MLX5 hardware requirement: CQE (Completion Queue Entry) must be
    // initialized to 0xFF (-1) to mark them as invalid. The hardware checks the
    // owner bit in CQE to determine if it's valid. This is mandatory for proper
    // CQ operation. Use async version to avoid blocking.
    if (cudaMemsetAsync(ctrl_buf + cq_offset, -1,
                        num_cqe * sizeof(struct mlx5_cqe64),
                        stream) != cudaSuccess) {
        print_cuda_error("Failed to memset CQ memory");
        goto fail;
    }
    dbr_offset = memheap_alloc(ctrl_buf_heap, sizeof(struct mlx5gda_cq_dbr));
    if (dbr_offset == -1) {
        perror("Failed to allocate DBR memory");
        goto fail;
    }
    cq = (struct mlx5gda_cq *)malloc(sizeof(struct mlx5gda_cq));
    if (!cq) goto fail;
    if (mlx5dv_devx_query_eqn(ctx, 0, &eqn)) {
        perror("Failed to query EQN");
        goto fail;
    }
    uar = mlx5dv_devx_alloc_uar(
        ctx,
        MLX5DV_UAR_ALLOC_TYPE_NC);  // cq uar is required but we don't use it
    if (!uar) {
        perror("Failed to create UAR");
        goto fail;
    }

    DEVX_SET(create_cq_in, cmd_in, opcode, MLX5_CMD_OP_CREATE_CQ);
    DEVX_SET(create_cq_in, cmd_in, cq_umem_id, ctrl_buf_umem->umem_id);
    DEVX_SET(create_cq_in, cmd_in, cq_umem_valid, 1);
    DEVX_SET64(create_cq_in, cmd_in, cq_umem_offset, cq_offset);
    cq_context = DEVX_ADDR_OF(create_cq_in, cmd_in, cq_context);
    DEVX_SET(cqc, cq_context, dbr_umem_valid, 1);
    DEVX_SET(cqc, cq_context, cqe_sz, MLX5_CQE_SIZE_64B);
    DEVX_SET(cqc, cq_context, cc, 0x1);  // collapsed cq
    DEVX_SET(cqc, cq_context, oi, 0x1);  // cq overrun
    DEVX_SET(cqc, cq_context, dbr_umem_id, ctrl_buf_umem->umem_id);
    DEVX_SET(cqc, cq_context, log_cq_size, __builtin_ctz(num_cqe));
    DEVX_SET(cqc, cq_context, uar_page, uar->page_id);
    DEVX_SET(cqc, cq_context, c_eqn, eqn);
    DEVX_SET64(cqc, cq_context, dbr_addr, dbr_offset);  // DBR offset

    // Synchronize stream before creating CQ object, as hardware will read the
    // CQE memory
    if (cudaStreamSynchronize(stream) != cudaSuccess) {
        print_cuda_error("Failed to synchronize stream before CQ creation");
        goto fail;
    }

    mlx5_cq = mlx5dv_devx_obj_create(ctx, cmd_in, sizeof(cmd_in), cmd_out,
                                     sizeof(cmd_out));
    if (mlx5_cq == NULL) {
        perror("Failed to create command queue");
        goto fail;
    }
    cqn = DEVX_GET(create_cq_out, cmd_out, cqn);

    cq->cq_offset = cq_offset;
    cq->dbr_offset = dbr_offset;
    cq->cqe = num_cqe;
    cq->cqn = cqn;
    cq->uar = uar;
    cq->mcq = mlx5_cq;
    return cq;
fail:
    int saved_errno = errno;
    if (uar) mlx5dv_devx_free_uar(uar);
    if (cq) free(cq);
    errno = saved_errno;
    return NULL;
}

void mlx5gda_destroy_cq(struct memheap *ctrl_buf_heap, struct mlx5gda_cq *cq) {
    if (!cq) return;
    if (cq->mcq) {
        mlx5dv_devx_obj_destroy(cq->mcq);
    }
    if (cq->uar) {
        mlx5dv_devx_free_uar(cq->uar);
    }
    memheap_free(ctrl_buf_heap, cq->cq_offset);
    memheap_free(ctrl_buf_heap, cq->dbr_offset);
    free(cq);
}

struct mlx5gda_qp *mlx5gda_create_rc_qp(struct mlx5dv_pd mpd, void *ctrl_buf,
                                        struct mlx5dv_devx_umem *ctrl_buf_umem,
                                        struct memheap *ctrl_buf_heap,
                                        struct ibv_pd *pd, int wqe,
                                        uint8_t port_num, cudaStream_t stream) {
    struct mlx5gda_qp *qp = NULL;
    struct mlx5gda_cq *send_cq = NULL;
    struct mlx5dv_devx_uar *uar = NULL;
    struct mlx5dv_devx_obj *mlx5_qp = NULL;
    size_t wq_offset = -1;
    size_t dbr_offset = -1;

    struct ibv_context *ctx = pd->context;
    void *qp_context = NULL;
    void *cap = NULL;
    uint32_t cqe_version = 0;

    if (wqe <= 0) {
        errno = EINVAL;
        return NULL;
    }
    uint32_t num_wqebb = IBGDA_ROUND_UP_POW2_OR_0((uint32_t)wqe);

    uint8_t cmd_in[DEVX_ST_SZ_BYTES(create_qp_in)] = {0};
    uint8_t cmd_out[DEVX_ST_SZ_BYTES(create_qp_out)] = {0};

    uint8_t cmd_cap_in[DEVX_ST_SZ_BYTES(query_hca_cap_in)] = {0};
    uint8_t cmd_cap_out[DEVX_ST_SZ_BYTES(query_hca_cap_out)] = {0};

    qp = (struct mlx5gda_qp *)calloc(1, sizeof(struct mlx5gda_qp));
    if (!qp) {
        perror("Failed to allocate QP memory");
        goto fail;
    }

    qp->port_num = port_num;
    if (ibv_query_port(ctx, port_num, &qp->port_attr) != 0) {
        perror("Failed to query port attributes");
        goto fail;
    }

    DEVX_SET(query_hca_cap_in, cmd_cap_in, opcode, MLX5_CMD_OP_QUERY_HCA_CAP);
    DEVX_SET(query_hca_cap_in, cmd_cap_in, op_mod,
             MLX5_SET_HCA_CAP_OP_MOD_GENERAL_DEVICE | (MLX5_CAP_GENERAL << 1) |
                 HCA_CAP_OPMOD_GET_CUR);

    if (mlx5dv_devx_general_cmd(ctx, cmd_cap_in, sizeof(cmd_cap_in),
                                cmd_cap_out, sizeof(cmd_cap_out))) {
        perror("mlx5dv_devx_general_cmd failed");
        goto fail;
    }

    cap = DEVX_ADDR_OF(query_hca_cap_out, cmd_cap_out, capability.cmd_hca_cap);
    cqe_version = DEVX_GET(cmd_hca_cap, cap, cqe_version);
    if (cqe_version != 1) {
        fprintf(stderr, "cqe_version=%d not supported\n", cqe_version);
        errno = ENOTSUP;
        goto fail;
    }

    // Create send_cq on GPU memory.
    send_cq = mlx5gda_create_cq(ctrl_buf, ctrl_buf_umem, ctrl_buf_heap, pd, wqe,
                                stream);
    if (send_cq == NULL) {
        perror("mlx5gda_create_cq failed");
        goto fail;
    }

    uar = create_uar(ctx);
    if (!uar) {
        perror("Failed to create UAR");
        goto fail;
    }

    wq_offset = memheap_aligned_alloc(ctrl_buf_heap,
                                      num_wqebb * sizeof(struct mlx5gda_wqebb),
                                      (size_t)1 << MLX5_ADAPTER_PAGE_SHIFT);
    if (wq_offset == -1) {
        perror("Failed to allocate WQ memory");
        goto fail;
    }

    dbr_offset = memheap_alloc(ctrl_buf_heap, sizeof(struct mlx5gda_wq_dbr));
    if (dbr_offset == -1) {
        perror("Failed to allocate DBR memory");
        goto fail;
    }
    // DBR must be zero-initialized. Use async version to avoid blocking.
    if (cudaMemsetAsync(ctrl_buf + dbr_offset, 0, sizeof(struct mlx5gda_wq_dbr),
                        stream) != cudaSuccess) {
        print_cuda_error("Failed to zero DBR memory");
        goto fail;
    }

    DEVX_SET(create_qp_in, cmd_in, opcode, MLX5_CMD_OP_CREATE_QP);
    DEVX_SET(create_qp_in, cmd_in, wq_umem_id,
             ctrl_buf_umem->umem_id);  // WQ buffer
    DEVX_SET64(create_qp_in, cmd_in, wq_umem_offset, wq_offset);
    DEVX_SET(create_qp_in, cmd_in, wq_umem_valid, 1);  // Enable wq_umem_id

    qp_context = DEVX_ADDR_OF(create_qp_in, cmd_in, qpc);
    DEVX_SET(qpc, qp_context, st, MLX5_QPC_ST_RC);
    DEVX_SET(qpc, qp_context, pm_state, MLX5_QPC_PM_STATE_MIGRATED);
    DEVX_SET(qpc, qp_context, pd, mpd.pdn);
    DEVX_SET(qpc, qp_context, uar_page, uar->page_id);  // BF register
#define MLX5_RQ_TYPE_ZERO_SIZE_RQ 0x3
    DEVX_SET(qpc, qp_context, rq_type,
             MLX5_RQ_TYPE_ZERO_SIZE_RQ);  // no receive queue
    DEVX_SET(qpc, qp_context, cqn_snd, send_cq->cqn);
    // DEVX_SET(qpc, qp_context, cqn_rcv, device->qp_shared_object.rcqn);
    DEVX_SET(qpc, qp_context, log_sq_size, IBGDA_ILOG2_OR0(num_wqebb));
    DEVX_SET(qpc, qp_context, log_rq_size, 0);
    DEVX_SET(qpc, qp_context, dbr_umem_valid, 1);  // Enable dbr_umem_id
    DEVX_SET64(qpc, qp_context, dbr_addr,
               dbr_offset);  // Offset of dbr_umem_id (behavior changed because
                             // of dbr_umem_valid)
    DEVX_SET(qpc, qp_context, dbr_umem_id,
             ctrl_buf_umem->umem_id);  // DBR buffer
    DEVX_SET(qpc, qp_context, user_index, 0);
    DEVX_SET(qpc, qp_context, page_offset, 0);

    // Synchronize stream before creating QP object, as hardware will read the
    // DBR memory
    if (cudaStreamSynchronize(stream) != cudaSuccess) {
        print_cuda_error("Failed to synchronize stream before QP creation");
        goto fail;
    }

    mlx5_qp = mlx5dv_devx_obj_create(ctx, cmd_in, sizeof(cmd_in), cmd_out,
                                     sizeof(cmd_out));
    if (mlx5_qp == NULL) {
        goto fail;
    }

    qp->mqp = mlx5_qp;
    qp->send_cq = send_cq;
    qp->uar = uar;
    qp->pd = pd;
    qp->qpn = DEVX_GET(create_qp_out, cmd_out, qpn);
    qp->num_wqebb = num_wqebb;
    qp->wq_offset = wq_offset;
    qp->dbr_offset = dbr_offset;
    return qp;

fail:
    int saved_errno = errno;
    if (mlx5_qp) {
        mlx5dv_devx_obj_destroy(mlx5_qp);
    }
    if (uar) {
        destroy_uar(uar);
    }
    if (send_cq) {
        mlx5gda_destroy_cq(ctrl_buf_heap, send_cq);
    }
    if (qp) {
        free(qp);
    }
    if (wq_offset != -1) {
        memheap_free(ctrl_buf_heap, wq_offset);
    }
    if (dbr_offset != -1) {
        memheap_free(ctrl_buf_heap, dbr_offset);
    }
    errno = saved_errno;
    return NULL;
}

int mlx5gda_modify_rc_qp_rst2init(struct mlx5gda_qp *qp, uint16_t pkey_index) {
    if (!qp || !qp->mqp) {
        errno = EINVAL;
        return -1;
    }
    uint8_t cmd_in[DEVX_ST_SZ_BYTES(rst2init_qp_in)] = {0};
    uint8_t cmd_out[DEVX_ST_SZ_BYTES(rst2init_qp_out)] = {0};

    DEVX_SET(rst2init_qp_in, cmd_in, opcode, MLX5_CMD_OP_RST2INIT_QP);
    DEVX_SET(rst2init_qp_in, cmd_in, qpn, qp->qpn);

    void *qpc = DEVX_ADDR_OF(rst2init_qp_in, cmd_in, qpc);

    DEVX_SET(qpc, qpc, rwe, 1);
    DEVX_SET(qpc, qpc, rre, 1);
    DEVX_SET(qpc, qpc, rae, 1);
    DEVX_SET(qpc, qpc, atomic_mode, 3);  // up to 64 bit

    DEVX_SET(qpc, qpc, primary_address_path.vhca_port_num, qp->port_num);

    if (qp->port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        DEVX_SET(qpc, qpc, primary_address_path.pkey_index, pkey_index);
    }

    DEVX_SET(qpc, qpc, pm_state, MLX5_QPC_PM_STATE_MIGRATED);

    int ret = mlx5dv_devx_obj_modify(qp->mqp, cmd_in, sizeof(cmd_in), cmd_out,
                                     sizeof(cmd_out));
    if (ret) {
        perror("Failed to modify RC QP (rst2init)");
    }
    return ret;
}

int mlx5gda_modify_rc_qp_init2rtr(struct mlx5gda_qp *qp,
                                  struct ibv_ah_attr ah_attr,
                                  uint32_t remote_qpn, enum ibv_mtu mtu) {
    if (!qp || !qp->mqp) {
        errno = EINVAL;
        return -1;
    }
    int ret = 0;
    struct ibv_ah *ah = NULL;
    uint8_t cmd_in[DEVX_ST_SZ_BYTES(init2rtr_qp_in)] = {0};
    uint8_t cmd_out[DEVX_ST_SZ_BYTES(init2rtr_qp_out)] = {0};

    DEVX_SET(rst2init_qp_in, cmd_in, opcode, MLX5_CMD_OP_INIT2RTR_QP);
    DEVX_SET(rst2init_qp_in, cmd_in, qpn, qp->qpn);

    void *qpc = DEVX_ADDR_OF(rst2init_qp_in, cmd_in, qpc);

    DEVX_SET(qpc, qpc, mtu, mtu);
    DEVX_SET(qpc, qpc, log_msg_max, 30);
    DEVX_SET(qpc, qpc, remote_qpn, remote_qpn);
    DEVX_SET(qpc, qpc, min_rnr_nak, 7);
    DEVX_SET(qpc, qpc, log_rra_max, 1);  // log2(max_rd_atomic)
    if (qp->port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        DEVX_SET(qpc, qpc, primary_address_path.sl,
                 ah_attr.sl);  // infiniband only
        DEVX_SET(qpc, qpc, primary_address_path.rlid, ah_attr.dlid);
    } else if (qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        struct mlx5dv_obj dv;
        struct mlx5dv_ah dah;

        ah = ibv_create_ah(qp->pd, &ah_attr);
        if (!ah) {
            perror("Failed to create ah");
            ret = -1;
            goto cleanup;
        }
        dv.ah.in = ah;
        dv.ah.out = &dah;
        mlx5dv_init_obj(&dv, MLX5DV_OBJ_AH);

        memcpy(DEVX_ADDR_OF(qpc, qpc, primary_address_path.rmac_47_32),
               &dah.av->rmac, sizeof(dah.av->rmac));
        DEVX_SET(qpc, qpc, primary_address_path.hop_limit, 255);
        DEVX_SET(qpc, qpc, primary_address_path.src_addr_index,
                 ah_attr.grh.sgid_index);
        DEVX_SET(qpc, qpc, primary_address_path.udp_sport, ah_attr.dlid);
        memcpy(DEVX_ADDR_OF(qpc, qpc, primary_address_path.rgid_rip),
               &dah.av->rgid, sizeof(dah.av->rgid));
    }

    ret = mlx5dv_devx_obj_modify(qp->mqp, cmd_in, sizeof(cmd_in), cmd_out,
                                 sizeof(cmd_out));
    if (ret) {
        perror("Failed to modify RC QP (init2rtr)");
    }

cleanup:
    if (ah) {
        ibv_destroy_ah(ah);
    }

    return ret;
}

int mlx5gda_modify_rc_qp_rtr2rts(struct mlx5gda_qp *qp) {
    if (!qp || !qp->mqp) {
        errno = EINVAL;
        return -1;
    }
    uint8_t cmd_in[DEVX_ST_SZ_BYTES(rtr2rts_qp_in)] = {0};
    uint8_t cmd_out[DEVX_ST_SZ_BYTES(rtr2rts_qp_out)] = {0};

    DEVX_SET(rst2init_qp_in, cmd_in, opcode, MLX5_CMD_OP_RTR2RTS_QP);
    DEVX_SET(rst2init_qp_in, cmd_in, qpn, qp->qpn);

    void *qpc = DEVX_ADDR_OF(rst2init_qp_in, cmd_in, qpc);

    DEVX_SET(qpc, qpc, log_ack_req_freq, 0x0);  // Ack every packet
    DEVX_SET(qpc, qpc, log_sra_max, 1);         // log2(max_qp_rd_atomic)
    DEVX_SET(qpc, qpc, next_send_psn, 0x0);
    DEVX_SET(qpc, qpc, retry_count, 7);
    DEVX_SET(qpc, qpc, rnr_retry, 7);
    DEVX_SET(qpc, qpc, primary_address_path.ack_timeout, 20);

    int ret = mlx5dv_devx_obj_modify(qp->mqp, cmd_in, sizeof(cmd_in), cmd_out,
                                     sizeof(cmd_out));
    if (ret) {
        perror("Failed to modify RC QP (rtr2rts)");
    }
    return ret;
}