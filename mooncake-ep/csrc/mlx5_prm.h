/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2016 6WIND S.A.
 * Copyright 2016 Mellanox Technologies, Ltd
 */

#ifndef RTE_PMD_MLX5_PRM_H_
#define RTE_PMD_MLX5_PRM_H_

#include <stdint.h>

#define u8 uint8_t

#define MLX5_ADAPTER_PAGE_SHIFT 12

enum {
    MLX5_CQE_SIZE_64B = 0x0,
    MLX5_CQE_SIZE_128B = 0x1,
};

struct mlx5_ifc_cqc_bits {
    u8 status[0x4];
    u8 as_notify[0x1];
    u8 initiator_src_dct[0x1];
    u8 dbr_umem_valid[0x1];
    u8 reserved_at_7[0x1];
    u8 cqe_sz[0x3];
    u8 cc[0x1];
    u8 reserved_at_c[0x1];
    u8 scqe_break_moderation_en[0x1];
    u8 oi[0x1];
    u8 cq_period_mode[0x2];
    u8 cqe_comp_en[0x1];
    u8 mini_cqe_res_format[0x2];
    u8 st[0x4];
    u8 reserved_at_18[0x1];
    u8 cqe_comp_layout[0x7];
    u8 dbr_umem_id[0x20];
    u8 reserved_at_40[0x14];
    u8 page_offset[0x6];
    u8 reserved_at_5a[0x2];
    u8 mini_cqe_res_format_ext[0x2];
    u8 cq_timestamp_format[0x2];
    u8 reserved_at_60[0x3];
    u8 log_cq_size[0x5];
    u8 uar_page[0x18];
    u8 reserved_at_80[0x4];
    u8 cq_period[0xc];
    u8 cq_max_count[0x10];
    u8 reserved_at_a0[0x18];
    u8 c_eqn[0x8];
    u8 reserved_at_c0[0x3];
    u8 log_page_size[0x5];
    u8 reserved_at_c8[0x18];
    u8 reserved_at_e0[0x20];
    u8 reserved_at_100[0x8];
    u8 last_notified_index[0x18];
    u8 reserved_at_120[0x8];
    u8 last_solicit_index[0x18];
    u8 reserved_at_140[0x8];
    u8 consumer_counter[0x18];
    u8 reserved_at_160[0x8];
    u8 producer_counter[0x18];
    u8 local_partition_id[0xc];
    u8 process_id[0x14];
    u8 reserved_at_1A0[0x20];
    u8 dbr_addr[0x40];
};

struct mlx5_ifc_create_cq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];
    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];
    u8 reserved_at_40[0x40];
    struct mlx5_ifc_cqc_bits cq_context;
    u8 cq_umem_offset[0x40];
    u8 cq_umem_id[0x20];
    u8 cq_umem_valid[0x1];
    u8 reserved_at_2e1[0x1f];
    u8 reserved_at_300[0x580];
    u8 pas[];
};

#endif /* RTE_PMD_MLX5_PRM_H_ */
