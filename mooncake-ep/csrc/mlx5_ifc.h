/*
 * Copyright (c) 2019 Mellanox Technologies, Inc.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef MLX5_IFC_H
#define MLX5_IFC_H

#include <stdint.h>

#define u8 uint8_t

enum mlx5_cap_mode {
    HCA_CAP_OPMOD_GET_MAX = 0,
    HCA_CAP_OPMOD_GET_CUR = 1,
};

enum {
    MLX5_CMD_OP_QUERY_HCA_CAP = 0x100,
    MLX5_CMD_OP_INIT_HCA = 0x102,
    MLX5_CMD_OP_TEARDOWN_HCA = 0x103,
    MLX5_CMD_OP_ENABLE_HCA = 0x104,
    MLX5_CMD_OP_QUERY_PAGES = 0x107,
    MLX5_CMD_OP_MANAGE_PAGES = 0x108,
    MLX5_CMD_OP_SET_HCA_CAP = 0x109,
    MLX5_CMD_OP_QUERY_ISSI = 0x10a,
    MLX5_CMD_OP_SET_ISSI = 0x10b,
    MLX5_CMD_OP_CREATE_MKEY = 0x200,
    MLX5_CMD_OP_DESTROY_MKEY = 0x202,
    MLX5_CMD_OP_CREATE_EQ = 0x301,
    MLX5_CMD_OP_DESTROY_EQ = 0x302,
    MLX5_CMD_OP_CREATE_CQ = 0x400,
    MLX5_CMD_OP_DESTROY_CQ = 0x401,
    MLX5_CMD_OP_CREATE_QP = 0x500,
    MLX5_CMD_OP_DESTROY_QP = 0x501,
    MLX5_CMD_OP_RST2INIT_QP = 0x502,
    MLX5_CMD_OP_INIT2RTR_QP = 0x503,
    MLX5_CMD_OP_RTR2RTS_QP = 0x504,
    MLX5_CMD_OP_RTS2RTS_QP = 0x505,
    MLX5_CMD_OP_QUERY_QP = 0x50b,
    MLX5_CMD_OP_INIT2INIT_QP = 0x50e,
    MLX5_CMD_OP_CREATE_PSV = 0x600,
    MLX5_CMD_OP_DESTROY_PSV = 0x601,
    MLX5_CMD_OP_CREATE_SRQ = 0x700,
    MLX5_CMD_OP_DESTROY_SRQ = 0x701,
    MLX5_CMD_OP_CREATE_XRC_SRQ = 0x705,
    MLX5_CMD_OP_DESTROY_XRC_SRQ = 0x706,
    MLX5_CMD_OP_CREATE_DCT = 0x710,
    MLX5_CMD_OP_DESTROY_DCT = 0x711,
    MLX5_CMD_OP_QUERY_DCT = 0x713,
    MLX5_CMD_OP_CREATE_XRQ = 0x717,
    MLX5_CMD_OP_DESTROY_XRQ = 0x718,
    MLX5_CMD_OP_QUERY_ESW_FUNCTIONS = 0x740,
    MLX5_CMD_OP_QUERY_ESW_VPORT_CONTEXT = 0x752,
    MLX5_CMD_OP_QUERY_NIC_VPORT_CONTEXT = 0x754,
    MLX5_CMD_OP_MODIFY_NIC_VPORT_CONTEXT = 0x755,
    MLX5_CMD_OP_QUERY_ROCE_ADDRESS = 0x760,
    MLX5_CMD_OP_ALLOC_Q_COUNTER = 0x771,
    MLX5_CMD_OP_DEALLOC_Q_COUNTER = 0x772,
    MLX5_CMD_OP_CREATE_SCHEDULING_ELEMENT = 0x782,
    MLX5_CMD_OP_DESTROY_SCHEDULING_ELEMENT = 0x783,
    MLX5_CMD_OP_ALLOC_PD = 0x800,
    MLX5_CMD_OP_DEALLOC_PD = 0x801,
    MLX5_CMD_OP_ALLOC_UAR = 0x802,
    MLX5_CMD_OP_DEALLOC_UAR = 0x803,
    MLX5_CMD_OP_ACCESS_REG = 0x805,
    MLX5_CMD_OP_ATTACH_TO_MCG = 0x806,
    MLX5_CMD_OP_DETACH_FROM_MCG = 0x807,
    MLX5_CMD_OP_ALLOC_XRCD = 0x80e,
    MLX5_CMD_OP_DEALLOC_XRCD = 0x80f,
    MLX5_CMD_OP_ALLOC_TRANSPORT_DOMAIN = 0x816,
    MLX5_CMD_OP_DEALLOC_TRANSPORT_DOMAIN = 0x817,
    MLX5_CMD_OP_ADD_VXLAN_UDP_DPORT = 0x827,
    MLX5_CMD_OP_DELETE_VXLAN_UDP_DPORT = 0x828,
    MLX5_CMD_OP_SET_L2_TABLE_ENTRY = 0x829,
    MLX5_CMD_OP_DELETE_L2_TABLE_ENTRY = 0x82b,
    MLX5_CMD_OP_QUERY_LAG = 0x842,
    MLX5_CMD_OP_CREATE_TIR = 0x900,
    MLX5_CMD_OP_DESTROY_TIR = 0x902,
    MLX5_CMD_OP_CREATE_SQ = 0x904,
    MLX5_CMD_OP_MODIFY_SQ = 0x905,
    MLX5_CMD_OP_DESTROY_SQ = 0x906,
    MLX5_CMD_OP_CREATE_RQ = 0x908,
    MLX5_CMD_OP_DESTROY_RQ = 0x90a,
    MLX5_CMD_OP_CREATE_RMP = 0x90c,
    MLX5_CMD_OP_DESTROY_RMP = 0x90e,
    MLX5_CMD_OP_CREATE_TIS = 0x912,
    MLX5_CMD_OP_MODIFY_TIS = 0x913,
    MLX5_CMD_OP_DESTROY_TIS = 0x914,
    MLX5_CMD_OP_QUERY_TIS = 0x915,
    MLX5_CMD_OP_CREATE_RQT = 0x916,
    MLX5_CMD_OP_DESTROY_RQT = 0x918,
    MLX5_CMD_OP_CREATE_FLOW_TABLE = 0x930,
    MLX5_CMD_OP_DESTROY_FLOW_TABLE = 0x931,
    MLX5_CMD_OP_QUERY_FLOW_TABLE = 0x932,
    MLX5_CMD_OP_CREATE_FLOW_GROUP = 0x933,
    MLX5_CMD_OP_DESTROY_FLOW_GROUP = 0x934,
    MLX5_CMD_OP_SET_FLOW_TABLE_ENTRY = 0x936,
    MLX5_CMD_OP_DELETE_FLOW_TABLE_ENTRY = 0x938,
    MLX5_CMD_OP_CREATE_FLOW_COUNTER = 0x939,
    MLX5_CMD_OP_DEALLOC_FLOW_COUNTER = 0x93a,
    MLX5_CMD_OP_ALLOC_PACKET_REFORMAT_CONTEXT = 0x93d,
    MLX5_CMD_OP_DEALLOC_PACKET_REFORMAT_CONTEXT = 0x93e,
    MLX5_CMD_OP_ALLOC_MODIFY_HEADER_CONTEXT = 0x940,
    MLX5_CMD_OP_DEALLOC_MODIFY_HEADER_CONTEXT = 0x941,
    MLX5_CMD_OP_CREATE_GENERAL_OBJECT = 0xa00,
    MLX5_CMD_OP_MODIFY_GENERAL_OBJECT = 0xa01,
    MLX5_CMD_OP_QUERY_GENERAL_OBJECT = 0xa02,
    MLX5_CMD_OP_DESTROY_GENERAL_OBJECT = 0xa03,
    MLX5_CMD_OP_CREATE_UMEM = 0xa08,
    MLX5_CMD_OP_DESTROY_UMEM = 0xa0a,
    MLX5_CMD_OP_SYNC_STEERING = 0xb00,
};

enum {
    MLX5_CMD_STAT_OK = 0x0,
    MLX5_CMD_STAT_INT_ERR = 0x1,
    MLX5_CMD_STAT_BAD_OP_ERR = 0x2,
    MLX5_CMD_STAT_BAD_PARAM_ERR = 0x3,
    MLX5_CMD_STAT_BAD_SYS_STATE_ERR = 0x4,
    MLX5_CMD_STAT_BAD_RES_ERR = 0x5,
    MLX5_CMD_STAT_RES_BUSY = 0x6,
    MLX5_CMD_STAT_LIM_ERR = 0x8,
    MLX5_CMD_STAT_BAD_RES_STATE_ERR = 0x9,
    MLX5_CMD_STAT_IX_ERR = 0xa,
    MLX5_CMD_STAT_NO_RES_ERR = 0xf,
    MLX5_CMD_STAT_BAD_INP_LEN_ERR = 0x50,
    MLX5_CMD_STAT_BAD_OUTP_LEN_ERR = 0x51,
    MLX5_CMD_STAT_BAD_QP_STATE_ERR = 0x10,
    MLX5_CMD_STAT_BAD_PKT_ERR = 0x30,
    MLX5_CMD_STAT_BAD_SIZE_OUTS_CQES_ERR = 0x40,
};

enum {
    MLX5_PAGES_CANT_GIVE = 0,
    MLX5_PAGES_GIVE = 1,
    MLX5_PAGES_TAKE = 2,
};

enum {
    MLX5_REG_HOST_ENDIANNESS = 0x7004,
};

enum {
    MLX5_CAP_PORT_TYPE_IB = 0x0,
    MLX5_CAP_PORT_TYPE_ETH = 0x1,
};

enum mlx5_event {
    MLX5_EVENT_TYPE_CMD = 0x0a,
    MLX5_EVENT_TYPE_PAGE_REQUEST = 0xb,
};

enum {
    MLX5_EQ_DOORBEL_OFFSET = 0x40,
};

struct mlx5_ifc_atomic_caps_bits {
    u8 reserved_at_0[0x40];

    u8 atomic_req_8B_endianness_mode[0x2];
    u8 reserved_at_42[0x4];
    u8 supported_atomic_req_8B_endianness_mode_1[0x1];

    u8 reserved_at_47[0x19];

    u8 reserved_at_60[0x20];

    u8 reserved_at_80[0x10];
    u8 atomic_operations[0x10];

    u8 reserved_at_a0[0x10];
    u8 atomic_size_qp[0x10];

    u8 reserved_at_c0[0x10];
    u8 atomic_size_dc[0x10];

    u8 reserved_at_e0[0x1a0];

    u8 fetch_add_pci_atomic[0x10];
    u8 swap_pci_atomic[0x10];
    u8 compare_swap_pci_atomic[0x10];

    u8 reserved_at_2b0[0x550];
};

struct mlx5_ifc_roce_cap_bits {
    u8 reserved_0[0x4];
    u8 sw_r_roce_src_udp_port[0x1];
    u8 fl_rc_qp_when_roce_disabled[0x1];
    u8 fl_rc_qp_when_roce_enabled[0x1];
    u8 reserved_at_7[0x17];
    u8 qp_ts_format[0x2];

    u8 reserved_at_20[0x7e0];
};

enum {
    MLX5_MULTI_PATH_FT_MAX_LEVEL = 64,
};

struct mlx5_ifc_flow_table_context_bits {
    u8 reformat_en[0x1];
    u8 decap_en[0x1];
    u8 sw_owner[0x1];
    u8 termination_table[0x1];
    u8 table_miss_action[0x4];
    u8 level[0x8];
    u8 reserved_at_10[0x8];
    u8 log_size[0x8];

    u8 reserved_at_20[0x8];
    u8 table_miss_id[0x18];

    u8 reserved_at_40[0x8];
    u8 lag_master_next_table_id[0x18];

    u8 reserved_at_60[0x60];

    u8 sw_owner_icm_root_1[0x40];

    u8 sw_owner_icm_root_0[0x40];
};

struct mlx5_ifc_create_flow_table_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_flow_table_context_bits flow_table_context;
};

struct mlx5_ifc_create_flow_table_out_bits {
    u8 status[0x8];
    u8 icm_address_63_40[0x18];

    u8 syndrome[0x20];

    u8 icm_address_39_32[0x8];
    u8 table_id[0x18];

    u8 icm_address_31_0[0x20];
};

struct mlx5_ifc_destroy_flow_table_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 reserved_at_c0[0x140];
};

struct mlx5_ifc_query_flow_table_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 reserved_at_c0[0x140];
};

struct mlx5_ifc_query_flow_table_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x80];

    struct mlx5_ifc_flow_table_context_bits flow_table_context;
};

struct mlx5_ifc_sync_steering_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0xc0];
};

struct mlx5_ifc_sync_steering_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_device_mem_cap_bits {
    u8 memic[0x1];
    u8 reserved_at_1[0x1f];

    u8 reserved_at_20[0xb];
    u8 log_min_memic_alloc_size[0x5];
    u8 reserved_at_30[0x8];
    u8 log_max_memic_addr_alignment[0x8];

    u8 memic_bar_start_addr[0x40];

    u8 memic_bar_size[0x20];

    u8 max_memic_size[0x20];

    u8 steering_sw_icm_start_address[0x40];

    u8 reserved_at_100[0x8];
    u8 log_header_modify_sw_icm_size[0x8];
    u8 reserved_at_110[0x2];
    u8 log_sw_icm_alloc_granularity[0x6];
    u8 log_steering_sw_icm_size[0x8];

    u8 reserved_at_120[0x20];

    u8 header_modify_sw_icm_start_address[0x40];
};

struct mlx5_ifc_flow_table_fields_supported_bits {
    u8 outer_dmac[0x1];
    u8 outer_smac[0x1];
    u8 outer_ether_type[0x1];
    u8 outer_ip_version[0x1];
    u8 outer_first_prio[0x1];
    u8 outer_first_cfi[0x1];
    u8 outer_first_vid[0x1];
    u8 outer_ipv4_ttl[0x1];
    u8 outer_second_prio[0x1];
    u8 outer_second_cfi[0x1];
    u8 outer_second_vid[0x1];
    u8 outer_ipv6_flow_label[0x1];
    u8 outer_sip[0x1];
    u8 outer_dip[0x1];
    u8 outer_frag[0x1];
    u8 outer_ip_protocol[0x1];
    u8 outer_ip_ecn[0x1];
    u8 outer_ip_dscp[0x1];
    u8 outer_udp_sport[0x1];
    u8 outer_udp_dport[0x1];
    u8 outer_tcp_sport[0x1];
    u8 outer_tcp_dport[0x1];
    u8 outer_tcp_flags[0x1];
    u8 outer_gre_protocol[0x1];
    u8 outer_gre_key[0x1];
    u8 outer_vxlan_vni[0x1];
    u8 outer_geneve_vni[0x1];
    u8 outer_geneve_oam[0x1];
    u8 outer_geneve_protocol_type[0x1];
    u8 outer_geneve_opt_len[0x1];
    u8 source_vhca_port[0x1];
    u8 source_eswitch_port[0x1];

    u8 inner_dmac[0x1];
    u8 inner_smac[0x1];
    u8 inner_ether_type[0x1];
    u8 inner_ip_version[0x1];
    u8 inner_first_prio[0x1];
    u8 inner_first_cfi[0x1];
    u8 inner_first_vid[0x1];
    u8 inner_ipv4_ttl[0x1];
    u8 inner_second_prio[0x1];
    u8 inner_second_cfi[0x1];
    u8 inner_second_vid[0x1];
    u8 inner_ipv6_flow_label[0x1];
    u8 inner_sip[0x1];
    u8 inner_dip[0x1];
    u8 inner_frag[0x1];
    u8 inner_ip_protocol[0x1];
    u8 inner_ip_ecn[0x1];
    u8 inner_ip_dscp[0x1];
    u8 inner_udp_sport[0x1];
    u8 inner_udp_dport[0x1];
    u8 inner_tcp_sport[0x1];
    u8 inner_tcp_dport[0x1];
    u8 inner_tcp_flags[0x1];
    u8 reserved_at_37[0x7];
    u8 metadata_reg_b[0x1];
    u8 metadata_reg_a[0x1];

    u8 reserved_at_40[0x5];
    u8 outer_first_mpls_over_udp_ttl[0x1];
    u8 outer_first_mpls_over_udp_s_bos[0x1];
    u8 outer_first_mpls_over_udp_exp[0x1];
    u8 outer_first_mpls_over_udp_label[0x1];
    u8 outer_first_mpls_over_gre_ttl[0x1];
    u8 outer_first_mpls_over_gre_s_bos[0x1];
    u8 outer_first_mpls_over_gre_exp[0x1];
    u8 outer_first_mpls_over_gre_label[0x1];
    u8 inner_first_mpls_ttl[0x1];
    u8 inner_first_mpls_s_bos[0x1];
    u8 inner_first_mpls_exp[0x1];
    u8 inner_first_mpls_label[0x1];
    u8 outer_first_mpls_ttl[0x1];
    u8 outer_first_mpls_s_bos[0x1];
    u8 outer_first_mpls_exp[0x1];
    u8 outer_first_mpls_label[0x1];
    u8 outer_emd_tag[0x1];
    u8 inner_esp_spi[0x1];
    u8 outer_esp_spi[0x1];
    u8 inner_ipv6_hop_limit[0x1];
    u8 outer_ipv6_hop_limit[0x1];
    u8 bth_dst_qp[0x1];
    u8 inner_first_svlan[0x1];
    u8 inner_second_svlan[0x1];
    u8 outer_first_svlan[0x1];
    u8 outer_second_svlan[0x1];
    u8 source_sqn[0x1];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dr_match_spec_bits {
    u8 smac_47_16[0x20];

    u8 smac_15_0[0x10];
    u8 ethertype[0x10];

    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 first_prio[0x3];
    u8 first_cfi[0x1];
    u8 first_vid[0xc];

    u8 ip_protocol[0x8];
    u8 ip_dscp[0x6];
    u8 ip_ecn[0x2];
    u8 cvlan_tag[0x1];
    u8 svlan_tag[0x1];
    u8 frag[0x1];
    u8 ip_version[0x4];
    u8 tcp_flags[0x9];

    u8 tcp_sport[0x10];
    u8 tcp_dport[0x10];

    u8 reserved_at_c0[0x10];
    u8 ipv4_ihl[0x4];
    u8 l3_ok[0x1];
    u8 l4_ok[0x1];
    u8 ipv4_checksum_ok[0x1];
    u8 l4_checksum_ok[0x1];
    u8 ip_ttl_hoplimit[0x8];

    u8 udp_sport[0x10];
    u8 udp_dport[0x10];

    u8 src_ip_127_96[0x20];

    u8 src_ip_95_64[0x20];

    u8 src_ip_63_32[0x20];

    u8 src_ip_31_0[0x20];

    u8 dst_ip_127_96[0x20];

    u8 dst_ip_95_64[0x20];

    u8 dst_ip_63_32[0x20];

    u8 dst_ip_31_0[0x20];
};

struct mlx5_ifc_dr_match_set_misc_bits {
    u8 gre_c_present[0x1];
    u8 reserved_auto1[0x1];
    u8 gre_k_present[0x1];
    u8 gre_s_present[0x1];
    u8 source_vhca_port[0x4];
    u8 source_sqn[0x18];

    u8 source_eswitch_owner_vhca_id[0x10];
    u8 source_port[0x10];

    u8 outer_second_prio[0x3];
    u8 outer_second_cfi[0x1];
    u8 outer_second_vid[0xc];
    u8 inner_second_prio[0x3];
    u8 inner_second_cfi[0x1];
    u8 inner_second_vid[0xc];

    u8 outer_second_cvlan_tag[0x1];
    u8 inner_second_cvlan_tag[0x1];
    u8 outer_second_svlan_tag[0x1];
    u8 inner_second_svlan_tag[0x1];
    u8 outer_emd_tag[0x1];
    u8 reserved_at_65[0xb];
    u8 gre_protocol[0x10];

    u8 gre_key_h[0x18];
    u8 gre_key_l[0x8];

    u8 vxlan_vni[0x18];
    u8 reserved_at_b8[0x8];

    u8 geneve_vni[0x18];
    u8 reserved_at_e4[0x7];
    u8 geneve_oam[0x1];

    u8 reserved_at_ec[0xc];
    u8 outer_ipv6_flow_label[0x14];

    u8 reserved_at_100[0xc];
    u8 inner_ipv6_flow_label[0x14];

    u8 reserved_at_120[0xa];
    u8 geneve_opt_len[0x6];
    u8 geneve_protocol_type[0x10];

    u8 reserved_at_140[0x8];
    u8 bth_dst_qp[0x18];

    u8 inner_esp_spi[0x20];

    u8 outer_esp_spi[0x20];

    u8 reserved_at_1a0[0x60];
};

struct mlx5_ifc_dr_match_set_misc2_bits {
    u8 outer_first_mpls_label[0x14];
    u8 outer_first_mpls_exp[0x3];
    u8 outer_first_mpls_s_bos[0x1];
    u8 outer_first_mpls_ttl[0x8];

    u8 inner_first_mpls_label[0x14];
    u8 inner_first_mpls_exp[0x3];
    u8 inner_first_mpls_s_bos[0x1];
    u8 inner_first_mpls_ttl[0x8];

    u8 outer_first_mpls_over_gre_label[0x14];
    u8 outer_first_mpls_over_gre_exp[0x3];
    u8 outer_first_mpls_over_gre_s_bos[0x1];
    u8 outer_first_mpls_over_gre_ttl[0x8];

    u8 outer_first_mpls_over_udp_label[0x14];
    u8 outer_first_mpls_over_udp_exp[0x3];
    u8 outer_first_mpls_over_udp_s_bos[0x1];
    u8 outer_first_mpls_over_udp_ttl[0x8];

    u8 metadata_reg_c_7[0x20];
    u8 metadata_reg_c_6[0x20];
    u8 metadata_reg_c_5[0x20];
    u8 metadata_reg_c_4[0x20];
    u8 metadata_reg_c_3[0x20];
    u8 metadata_reg_c_2[0x20];
    u8 metadata_reg_c_1[0x20];
    u8 metadata_reg_c_0[0x20];

    u8 metadata_reg_a[0x20];
    u8 metadata_reg_b[0x20];

    u8 reserved_at_260[0x40];
};

struct mlx5_ifc_dr_match_set_misc3_bits {
    u8 inner_tcp_seq_num[0x20];

    u8 outer_tcp_seq_num[0x20];

    u8 inner_tcp_ack_num[0x20];

    u8 outer_tcp_ack_num[0x20];

    u8 reserved_at_80[0x8];
    u8 outer_vxlan_gpe_vni[0x18];

    u8 outer_vxlan_gpe_next_protocol[0x8];
    u8 outer_vxlan_gpe_flags[0x8];
    u8 reserved_at_b0[0x10];

    u8 icmp_header_data[0x20];

    u8 icmpv6_header_data[0x20];

    u8 icmp_type[0x8];
    u8 icmp_code[0x8];
    u8 icmpv6_type[0x8];
    u8 icmpv6_code[0x8];

    u8 geneve_tlv_option_0_data[0x20];

    u8 gtpu_teid[0x20];

    u8 gtpu_msg_type[0x8];
    u8 gtpu_msg_flags[0x8];
    u8 reserved_at_150[0x10];

    u8 gtpu_dw_2[0x20];

    u8 gtpu_first_ext_dw_0[0x20];

    u8 gtpu_dw_0[0x20];

    u8 reserved_at_1c0[0x20];
};

struct mlx5_ifc_dr_match_set_misc4_bits {
    u8 prog_sample_field_value_0[0x20];

    u8 prog_sample_field_id_0[0x20];

    u8 prog_sample_field_value_1[0x20];

    u8 prog_sample_field_id_1[0x20];

    u8 prog_sample_field_value_2[0x20];

    u8 prog_sample_field_id_2[0x20];

    u8 prog_sample_field_value_3[0x20];

    u8 prog_sample_field_id_3[0x20];

    u8 prog_sample_field_value_4[0x20];

    u8 prog_sample_field_id_4[0x20];

    u8 prog_sample_field_value_5[0x20];

    u8 prog_sample_field_id_5[0x20];

    u8 prog_sample_field_value_6[0x20];

    u8 prog_sample_field_id_6[0x20];

    u8 prog_sample_field_value_7[0x20];

    u8 prog_sample_field_id_7[0x20];
};

struct mlx5_ifc_dr_match_set_misc5_bits {
    u8 macsec_tag_0[0x20];

    u8 macsec_tag_1[0x20];

    u8 macsec_tag_2[0x20];

    u8 macsec_tag_3[0x20];

    u8 tunnel_header_0[0x20];

    u8 tunnel_header_1[0x20];

    u8 tunnel_header_2[0x20];

    u8 tunnel_header_3[0x20];

    u8 reserved[0x100];
};

struct mlx5_ifc_dr_match_param_bits {
    struct mlx5_ifc_dr_match_spec_bits outer;
    struct mlx5_ifc_dr_match_set_misc_bits misc;
    struct mlx5_ifc_dr_match_spec_bits inner;
    struct mlx5_ifc_dr_match_set_misc2_bits misc2;
    struct mlx5_ifc_dr_match_set_misc3_bits misc3;
    struct mlx5_ifc_dr_match_set_misc4_bits misc4;
    struct mlx5_ifc_dr_match_set_misc5_bits misc5;
};

struct mlx5_ifc_flow_table_prop_layout_bits {
    u8 ft_support[0x1];
    u8 flow_tag[0x1];
    u8 flow_counter[0x1];
    u8 flow_modify_en[0x1];
    u8 modify_root[0x1];
    u8 identified_miss_table[0x1];
    u8 flow_table_modify[0x1];
    u8 reformat[0x1];
    u8 decap[0x1];
    u8 reset_root_to_default[0x1];
    u8 pop_vlan[0x1];
    u8 push_vlan[0x1];
    u8 fpga_vendor_acceleration[0x1];
    u8 pop_vlan_2[0x1];
    u8 push_vlan_2[0x1];
    u8 reformat_and_vlan_action[0x1];
    u8 modify_and_vlan_action[0x1];
    u8 sw_owner[0x1];
    u8 reformat_l3_tunnel_to_l2[0x1];
    u8 reformat_l2_to_l3_tunnel[0x1];
    u8 reformat_and_modify_action[0x1];
    u8 reserved_at_15[0x9];
    u8 sw_owner_v2[0x1];
    u8 reserved_at_1f[0x1];

    u8 reserved_at_20[0x2];
    u8 log_max_ft_size[0x6];
    u8 log_max_modify_header_context[0x8];
    u8 max_modify_header_actions[0x8];
    u8 max_ft_level[0x8];

    u8 reserved_at_40[0x10];
    u8 metadata_reg_b_width[0x8];
    u8 metadata_reg_a_width[0x8];

    u8 reserved_at_60[0x18];
    u8 log_max_ft_num[0x8];

    u8 reserved_at_80[0x10];
    u8 log_max_flow_counter[0x8];
    u8 log_max_destination[0x8];

    u8 reserved_at_a0[0x18];
    u8 log_max_flow[0x8];

    u8 reserved_at_c0[0x40];

    struct mlx5_ifc_flow_table_fields_supported_bits ft_field_support;

    struct mlx5_ifc_flow_table_fields_supported_bits ft_field_bitmask_support;
};

enum {
    MLX5_FLEX_PARSER_GENEVE_ENABLED = 1 << 3,
    MLX5_FLEX_PARSER_MPLS_OVER_GRE_ENABLED = 1 << 4,
    mlx5_FLEX_PARSER_MPLS_OVER_UDP_ENABLED = 1 << 5,
    MLX5_FLEX_PARSER_VXLAN_GPE_ENABLED = 1 << 7,
    MLX5_FLEX_PARSER_ICMP_V4_ENABLED = 1 << 8,
    MLX5_FLEX_PARSER_ICMP_V6_ENABLED = 1 << 9,
    MLX5_FLEX_PARSER_GENEVE_OPT_0_ENABLED = 1 << 10,
    MLX5_FLEX_PARSER_GTPU_ENABLED = 1 << 11,
    MLX5_FLEX_PARSER_GTPU_DW_2_ENABLED = 1 << 16,
    MLX5_FLEX_PARSER_GTPU_FIRST_EXT_DW_0_ENABLED = 1 << 17,
    MLX5_FLEX_PARSER_GTPU_DW_0_ENABLED = 1 << 18,
    MLX5_FLEX_PARSER_GTPU_TEID_ENABLED = 1 << 19,
};

enum mlx5_ifc_steering_format_version {
    MLX5_HW_CONNECTX_5 = 0x0,
    MLX5_HW_CONNECTX_6DX = 0x1,
};

enum mlx5_ifc_ste_v1_modify_hdr_offset {
    MLX5_MODIFY_HEADER_V1_QW_OFFSET = 0x20,
};

struct mlx5_ifc_cmd_hca_cap_bits {
    u8 access_other_hca_roce[0x1];
    u8 reserved_at_1[0x1e];
    u8 vhca_resource_manager[0x1];

    u8 hca_cap_2[0x1];
    u8 reserved_at_21[0xf];
    u8 vhca_id[0x10];

    u8 reserved_at_40[0x20];

    u8 reserved_at_60[0x2];
    u8 qp_data_in_order[0x1];
    u8 reserved_at_63[0x8];
    u8 log_dma_mmo_max_size[0x5];
    u8 reserved_at_70[0x10];

    u8 log_max_srq_sz[0x8];
    u8 log_max_qp_sz[0x8];
    u8 reserved_at_90[0x3];
    u8 isolate_vl_tc_new[0x1];
    u8 reserved_at_94[0x4];
    u8 prio_tag_required[0x1];
    u8 reserved_at_99[0x2];
    u8 log_max_qp[0x5];

    u8 reserved_at_a0[0xb];
    u8 log_max_srq[0x5];
    u8 reserved_at_b0[0x10];

    u8 reserved_at_c0[0x8];
    u8 log_max_cq_sz[0x8];
    u8 reserved_at_d0[0xb];
    u8 log_max_cq[0x5];

    u8 log_max_eq_sz[0x8];
    u8 relaxed_ordering_write[0x1];
    u8 reserved_at_e9[0x1];
    u8 log_max_mkey[0x6];
    u8 tunneled_atomic[0x1];
    u8 as_notify[0x1];
    u8 m_pci_port[0x1];
    u8 m_vhca_mk[0x1];
    u8 cmd_on_behalf[0x1];
    u8 device_emulation_manager[0x1];
    u8 terminate_scatter_list_mkey[0x1];
    u8 repeated_mkey[0x1];
    u8 dump_fill_mkey[0x1];
    u8 reserved_at_f9[0x3];
    u8 log_max_eq[0x4];

    u8 max_indirection[0x8];
    u8 fixed_buffer_size[0x1];
    u8 log_max_mrw_sz[0x7];
    u8 force_teardown[0x1];
    u8 fast_teardown[0x1];
    u8 log_max_bsf_list_size[0x6];
    u8 umr_extended_translation_offset[0x1];
    u8 null_mkey[0x1];
    u8 log_max_klm_list_size[0x6];

    u8 reserved_at_120[0x2];
    u8 qpc_extension[0x1];
    u8 reserved_at_123[0x7];
    u8 log_max_ra_req_dc[0x6];
    u8 reserved_at_130[0xa];
    u8 log_max_ra_res_dc[0x6];

    u8 reserved_at_140[0x7];
    u8 sig_crc64_xp10[0x1];
    u8 sig_crc32c[0x1];
    u8 reserved_at_149[0x1];
    u8 log_max_ra_req_qp[0x6];
    u8 reserved_at_150[0x1];
    u8 rts2rts_qp_udp_sport[0x1];
    u8 rts2rts_lag_tx_port_affinity[0x1];
    u8 dma_mmo_sq[0x1];
    u8 reserved_at_154[0x6];
    u8 log_max_ra_res_qp[0x6];

    u8 end_pad[0x1];
    u8 cc_query_allowed[0x1];
    u8 cc_modify_allowed[0x1];
    u8 start_pad[0x1];
    u8 cache_line_128byte[0x1];
    u8 gid_table_size_ro[0x1];
    u8 pkey_table_size_ro[0x1];
    u8 reserved_at_167[0x1];
    u8 rnr_nak_q_counters[0x1];
    u8 rts2rts_qp_counters_set_id[0x1];
    u8 rts2rts_qp_dscp[0x1];
    u8 reserved_at_16b[0x4];
    u8 qcam_reg[0x1];
    u8 gid_table_size[0x10];

    u8 out_of_seq_cnt[0x1];
    u8 vport_counters[0x1];
    u8 retransmission_q_counters[0x1];
    u8 debug[0x1];
    u8 modify_rq_counters_set_id[0x1];
    u8 rq_delay_drop[0x1];
    u8 max_qp_cnt[0xa];
    u8 pkey_table_size[0x10];

    u8 vport_group_manager[0x1];
    u8 vhca_group_manager[0x1];
    u8 ib_virt[0x1];
    u8 eth_virt[0x1];
    u8 vnic_env_queue_counters[0x1];
    u8 ets[0x1];
    u8 nic_flow_table[0x1];
    u8 eswitch_manager[0x1];
    u8 device_memory[0x1];
    u8 mcam_reg[0x1];
    u8 pcam_reg[0x1];
    u8 local_ca_ack_delay[0x5];
    u8 port_module_event[0x1];
    u8 enhanced_retransmission_q_counters[0x1];
    u8 port_checks[0x1];
    u8 pulse_gen_control[0x1];
    u8 disable_link_up_by_init_hca[0x1];
    u8 beacon_led[0x1];
    u8 port_type[0x2];
    u8 num_ports[0x8];

    u8 reserved_at_1c0[0x1];
    u8 pps[0x1];
    u8 pps_modify[0x1];
    u8 log_max_msg[0x5];
    u8 multi_path_xrc_rdma[0x1];
    u8 multi_path_dc_rdma[0x1];
    u8 multi_path_rc_rdma[0x1];
    u8 traffic_fast_control[0x1];
    u8 max_tc[0x4];
    u8 temp_warn_event[0x1];
    u8 dcbx[0x1];
    u8 general_notification_event[0x1];
    u8 multi_prio_sq[0x1];
    u8 afu_owner[0x1];
    u8 fpga[0x1];
    u8 rol_s[0x1];
    u8 rol_g[0x1];
    u8 ib_port_sniffer[0x1];
    u8 wol_s[0x1];
    u8 wol_g[0x1];
    u8 wol_a[0x1];
    u8 wol_b[0x1];
    u8 wol_m[0x1];
    u8 wol_u[0x1];
    u8 wol_p[0x1];

    u8 stat_rate_support[0x10];
    u8 sig_block_4048[0x1];
    u8 reserved_at_1f1[0xb];
    u8 cqe_version[0x4];

    u8 compact_address_vector[0x1];
    u8 eth_striding_wq[0x1];
    u8 reserved_at_202[0x1];
    u8 ipoib_enhanced_offloads[0x1];
    u8 ipoib_basic_offloads[0x1];
    u8 ib_striding_wq[0x1];
    u8 repeated_block_disabled[0x1];
    u8 umr_modify_entity_size_disabled[0x1];
    u8 umr_modify_atomic_disabled[0x1];
    u8 umr_indirect_mkey_disabled[0x1];
    u8 umr_fence[0x2];
    u8 dc_req_sctr_data_cqe[0x1];
    u8 dc_connect_qp[0x1];
    u8 dc_cnak_trace[0x1];
    u8 drain_sigerr[0x1];
    u8 cmdif_checksum[0x2];
    u8 sigerr_cqe[0x1];
    u8 reserved_at_213[0x1];
    u8 wq_signature[0x1];
    u8 sctr_data_cqe[0x1];
    u8 reserved_at_216[0x1];
    u8 sho[0x1];
    u8 tph[0x1];
    u8 rf[0x1];
    u8 dct[0x1];
    u8 qos[0x1];
    u8 eth_net_offloads[0x1];
    u8 roce[0x1];
    u8 atomic[0x1];
    u8 extended_retry_count[0x1];

    u8 cq_oi[0x1];
    u8 cq_resize[0x1];
    u8 cq_moderation[0x1];
    u8 cq_period_mode_modify[0x1];
    u8 cq_invalidate[0x1];
    u8 reserved_at_225[0x1];
    u8 cq_eq_remap[0x1];
    u8 pg[0x1];
    u8 block_lb_mc[0x1];
    u8 exponential_backoff[0x1];
    u8 scqe_break_moderation[0x1];
    u8 cq_period_start_from_cqe[0x1];
    u8 cd[0x1];
    u8 atm[0x1];
    u8 apm[0x1];
    u8 vector_calc[0x1];
    u8 umr_ptr_rlkey[0x1];
    u8 imaicl[0x1];
    u8 qp_packet_based[0x1];
    u8 reserved_at_233[0x1];
    u8 ipoib_enhanced_pkey_change[0x1];
    u8 initiator_src_dct_in_cqe[0x1];
    u8 qkv[0x1];
    u8 pkv[0x1];
    u8 set_deth_sqpn[0x1];
    u8 rts2rts_primary_sl[0x1];
    u8 initiator_src_dct[0x1];
    u8 dc_v2[0x1];
    u8 xrc[0x1];
    u8 ud[0x1];
    u8 uc[0x1];
    u8 rc[0x1];

    u8 uar_4k[0x1];
    u8 reserved_at_241[0x9];
    u8 uar_sz[0x6];
    u8 reserved_at_250[0x2];
    u8 umem_uid_0[0x1];
    u8 log_max_dc_cnak_qps[0x5];
    u8 log_pg_sz[0x8];

    u8 bf[0x1];
    u8 driver_version[0x1];
    u8 pad_tx_eth_packet[0x1];
    u8 query_driver_version[0x1];
    u8 max_qp_retry_freq[0x1];
    u8 qp_by_name[0x1];
    u8 mkey_by_name[0x1];
    u8 reserved_at_267[0x1];
    u8 suspend_qp_uc[0x1];
    u8 suspend_qp_ud[0x1];
    u8 suspend_qp_rc[0x1];
    u8 log_bf_reg_size[0x5];
    u8 reserved_at_270[0x6];
    u8 lag_dct[0x2];
    u8 lag_tx_port_affinity[0x1];
    u8 reserved_at_279[0x2];
    u8 lag_master[0x1];
    u8 num_lag_ports[0x4];

    u8 num_of_diagnostic_counters[0x10];
    u8 max_wqe_sz_sq[0x10];

    u8 reserved_at_2a0[0x10];
    u8 max_wqe_sz_rq[0x10];

    u8 max_flow_counter_31_16[0x10];
    u8 max_wqe_sz_sq_dc[0x10];

    u8 reserved_at_2e0[0x7];
    u8 max_qp_mcg[0x19];

    u8 mlnx_tag_ethertype[0x10];
    u8 reserved_at_310[0x8];
    u8 log_max_mcg[0x8];

    u8 reserved_at_320[0x3];
    u8 log_max_transport_domain[0x5];
    u8 reserved_at_328[0x3];
    u8 log_max_pd[0x5];
    u8 reserved_at_330[0xb];
    u8 log_max_xrcd[0x5];

    u8 nic_receive_steering_discard[0x1];
    u8 receive_discard_vport_down[0x1];
    u8 transmit_discard_vport_down[0x1];
    u8 eq_overrun_count[0x1];
    u8 nic_receive_steering_depth[0x1];
    u8 invalid_command_count[0x1];
    u8 quota_exceeded_count[0x1];
    u8 reserved_at_347[0x1];
    u8 log_max_flow_counter_bulk[0x8];
    u8 max_flow_counter_15_0[0x10];

    u8 modify_tis[0x1];
    u8 reserved_at_361[0x2];
    u8 log_max_rq[0x5];
    u8 reserved_at_368[0x3];
    u8 log_max_sq[0x5];
    u8 reserved_at_370[0x3];
    u8 log_max_tir[0x5];
    u8 reserved_at_378[0x3];
    u8 log_max_tis[0x5];

    u8 basic_cyclic_rcv_wqe[0x1];
    u8 reserved_at_381[0x2];
    u8 log_max_rmp[0x5];
    u8 reserved_at_388[0x3];
    u8 log_max_rqt[0x5];
    u8 reserved_at_390[0x3];
    u8 log_max_rqt_size[0x5];
    u8 reserved_at_398[0x3];
    u8 log_max_tis_per_sq[0x5];

    u8 ext_stride_num_range[0x1];
    u8 reserved_at_3a1[0x2];
    u8 log_max_stride_sz_rq[0x5];
    u8 reserved_at_3a8[0x3];
    u8 log_min_stride_sz_rq[0x5];
    u8 reserved_at_3b0[0x3];
    u8 log_max_stride_sz_sq[0x5];
    u8 reserved_at_3b8[0x3];
    u8 log_min_stride_sz_sq[0x5];

    u8 hairpin[0x1];
    u8 reserved_at_3c1[0x2];
    u8 log_max_hairpin_queues[0x5];
    u8 reserved_at_3c8[0x3];
    u8 log_max_hairpin_wq_data_sz[0x5];
    u8 reserved_at_3d0[0x3];
    u8 log_max_hairpin_num_packets[0x5];
    u8 reserved_at_3d8[0x3];
    u8 log_max_wq_sz[0x5];

    u8 nic_vport_change_event[0x1];
    u8 disable_local_lb_uc[0x1];
    u8 disable_local_lb_mc[0x1];
    u8 log_min_hairpin_wq_data_sz[0x5];
    u8 reserved_at_3e8[0x3];
    u8 log_max_vlan_list[0x5];
    u8 reserved_at_3f0[0x3];
    u8 log_max_current_mc_list[0x5];
    u8 reserved_at_3f8[0x3];
    u8 log_max_current_uc_list[0x5];

    u8 general_obj_types[0x40];

    u8 reserved_at_440[0x4];
    u8 steering_format_version[0x4];
    u8 create_qp_start_hint[0x18];

    u8 reserved_at_460[0x8];
    u8 aes_xts[0x1];
    u8 crypto[0x1];
    u8 reserved_at_46a[0x6];
    u8 max_num_eqs[0x10];

    u8 sigerr_domain_and_sig_type[0x1];
    u8 reserved_at_481[0x2];
    u8 log_max_l2_table[0x5];
    u8 reserved_at_488[0x8];
    u8 log_uar_page_sz[0x10];

    u8 reserved_at_4a0[0x20];

    u8 device_frequency_mhz[0x20];

    u8 device_frequency_khz[0x20];

    u8 capi[0x1];
    u8 create_pec[0x1];
    u8 nvmf_target_offload[0x1];
    u8 capi_invalidate[0x1];
    u8 reserved_at_504[0x17];
    u8 log_max_pasid[0x5];

    u8 num_of_uars_per_page[0x20];

    u8 flex_parser_protocols[0x20];

    u8 reserved_at_560[0x10];
    u8 flex_parser_header_modify[0x1];
    u8 reserved_at_571[0x2];
    u8 log_max_guaranteed_connections[0x5];
    u8 reserved_at_578[0x3];
    u8 log_max_dct_connections[0x5];

    u8 log_max_atomic_size_qp[0x8];
    u8 reserved_at_588[0x10];
    u8 log_max_atomic_size_dc[0x8];

    u8 reserved_at_5a0[0x1c];
    u8 mini_cqe_resp_stride_index[0x1];
    u8 cqe_128_always[0x1];
    u8 cqe_compression_128b[0x1];
    u8 cqe_compression[0x1];

    u8 cqe_compression_timeout[0x10];
    u8 cqe_compression_max_num[0x10];

    u8 reserved_at_5e0[0x8];
    u8 flex_parser_id_gtpu_dw_0[0x4];
    u8 log_max_tm_offloaded_op_size[0x4];
    u8 tag_matching[0x1];
    u8 rndv_offload_rc[0x1];
    u8 rndv_offload_dc[0x1];
    u8 log_tag_matching_list_sz[0x5];
    u8 reserved_at_5f8[0x3];
    u8 log_max_xrq[0x5];

    u8 affiliate_nic_vport_criteria[0x8];
    u8 native_port_num[0x8];
    u8 num_vhca_ports[0x8];
    u8 flex_parser_id_gtpu_teid[0x4];
    u8 reserved_at_61c[0x1];
    u8 trusted_vnic_vhca[0x1];
    u8 sw_owner_id[0x1];
    u8 reserve_not_to_use[0x1];
    u8 reserved_at_620[0x60];
    u8 sf[0x1];
    u8 reserved_at_682[0x43];
    u8 flex_parser_id_geneve_opt_0[0x4];
    u8 flex_parser_id_icmp_dw1[0x4];
    u8 flex_parser_id_icmp_dw0[0x4];
    u8 flex_parser_id_icmpv6_dw1[0x4];
    u8 flex_parser_id_icmpv6_dw0[0x4];
    u8 flex_parser_id_outer_first_mpls_over_gre[0x4];
    u8 flex_parser_id_outer_first_mpls_over_udp_label[0x4];

    u8 reserved_at_6e0[0x20];

    u8 flex_parser_id_gtpu_dw_2[0x4];
    u8 flex_parser_id_gtpu_first_ext_dw_0[0x4];
    u8 reserved_at_708[0x18];

    u8 reserved_at_720[0x20];

    u8 reserved_at_740[0x8];
    u8 dma_mmo_qp[0x1];
    u8 reserved_at_749[0x17];

    u8 reserved_at_760[0x60];

    u8 match_definer_format_supported[0x40];
};

struct mlx5_ifc_header_modify_cap_properties_bits {
    struct mlx5_ifc_flow_table_fields_supported_bits set_action_field_support;

    u8 reserved_at_80[0x80];

    struct mlx5_ifc_flow_table_fields_supported_bits add_action_field_support;

    u8 reserved_at_180[0x80];

    u8 copy_action_field_support[8][0x20];

    u8 reserved_at_300[0x100];
};

struct mlx5_ifc_flow_table_fields_supported_2_bits {
    u8 reserved_at_0[0x17];
    u8 inner_l3_ok[0x1];
    u8 inner_l4_ok[0x1];
    u8 outer_l3_ok[0x1];
    u8 outer_l4_ok[0x1];
    u8 psp_header[0x1];
    u8 inner_ipv4_checksum_ok[0x1];
    u8 inner_l4_checksum_ok[0x1];
    u8 outer_ipv4_checksum_ok[0x1];
    u8 outer_l4_checksum_ok[0x1];

    u8 reserved_at_20[0x60];
};

struct mlx5_ifc_flow_table_nic_cap_bits {
    u8 nic_rx_multi_path_tirs[0x1];
    u8 nic_rx_multi_path_tirs_fts[0x1];
    u8 allow_sniffer_and_nic_rx_shared_tir[0x1];
    u8 reserved_at_3[0x1];
    u8 nic_rx_flow_tag_multipath_en[0x1];
    u8 reserved_at_5[0x13];
    u8 nic_receive_max_steering_depth[0x8];

    u8 encap_general_header[0x1];
    u8 reserved_at_21[0xa];
    u8 log_max_packet_reformat_context[0x5];
    u8 reserved_at_30[0x6];
    u8 max_encap_header_size[0xa];

    u8 reserved_at_40[0x1c0];

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_receive;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_receive_rdma;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_receive_sniffer;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_transmit;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_transmit_rdma;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_transmit_sniffer;

    u8 reserved_at_e00[0x200];

    struct mlx5_ifc_header_modify_cap_properties_bits header_modify_nic_receive;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_receive;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_bitmask_support_2_nic_receive;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_receive_rdma;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_bitmask_support_2_nic_receive_rdma;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_receive_sniffer;

    struct mlx5_ifc_flow_table_fields_supported_2_bits
        ft_field_bitmask_support_2_nic_receive_sniffer;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_transmit;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_bitmask_support_2_nic_transmit;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_transmit_rdma;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_bitmask_support_2_nic_transmit_rdma;

    struct mlx5_ifc_flow_table_fields_supported_2_bits ft_field_support_2_nic_transmit_sniffer;

    struct mlx5_ifc_flow_table_fields_supported_2_bits
        ft_field_bitmask_support_2_nic_transmit_sniffer;

    u8 reserved_at_1400[0x200];

    struct mlx5_ifc_header_modify_cap_properties_bits header_modify_nic_transmit;

    u8 sw_steering_nic_rx_action_drop_icm_address[0x40];

    u8 sw_steering_nic_tx_action_drop_icm_address[0x40];

    u8 sw_steering_nic_tx_action_allow_icm_address[0x40];

    u8 reserved_at_20c0[0x5f40];
};

struct mlx5_ifc_flow_table_eswitch_cap_bits {
    u8 reserved_at_0[0x1c];
    u8 fdb_multi_path_to_table[0x1];
    u8 reserved_at_1d[0x1e3];

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_nic_esw_fdb;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_esw_acl_ingress;

    struct mlx5_ifc_flow_table_prop_layout_bits flow_table_properties_esw_acl_egress;

    u8 reserved_at_800[0x1000];

    u8 sw_steering_fdb_action_drop_icm_address_rx[0x40];
    u8 sw_steering_fdb_action_drop_icm_address_tx[0x40];
    u8 sw_steering_uplink_icm_address_rx[0x40];
    u8 sw_steering_uplink_icm_address_tx[0x40];

    u8 reserved_at_1900[0x6700];
};

struct mlx5_ifc_odp_per_transport_service_cap_bits {
    u8 send[0x1];
    u8 receive[0x1];
    u8 write[0x1];
    u8 read[0x1];
    u8 atomic[0x1];
    u8 srq_receive[0x1];
    u8 reserved_at_6[0x1a];
};

struct mlx5_ifc_odp_cap_bits {
    u8 reserved_at_0[0x40];

    u8 sig[0x1];
    u8 reserved_at_41[0x1f];

    u8 reserved_at_60[0x20];

    struct mlx5_ifc_odp_per_transport_service_cap_bits rc_odp_caps;

    struct mlx5_ifc_odp_per_transport_service_cap_bits uc_odp_caps;

    struct mlx5_ifc_odp_per_transport_service_cap_bits ud_odp_caps;

    struct mlx5_ifc_odp_per_transport_service_cap_bits xrc_odp_caps;

    struct mlx5_ifc_odp_per_transport_service_cap_bits dc_odp_caps;

    u8 reserved_at_120[0x6e0];
};

struct mlx5_ifc_e_switch_cap_bits {
    u8 reserved_at_0[0x4b];
    u8 log_max_esw_sf[0x5];
    u8 esw_sf_base_id[0x10];
    u8 reserved_at_60[0x7a0];
};

enum {
    ELEMENT_TYPE_CAP_MASK_TASR = 1 << 0,
    ELEMENT_TYPE_CAP_MASK_QUEUE_GROUP = 1 << 4,
};

enum {
    TSAR_TYPE_CAP_MASK_DWRR = 1 << 0,
};

struct mlx5_ifc_qos_cap_bits {
    u8 reserved_at_0[0x8];
    u8 nic_sq_scheduling[0x1];
    u8 nic_bw_share[0x1];
    u8 nic_rate_limit[0x1];
    u8 reserved_at_b[0x15];

    u8 reserved_at_20[0x1];
    u8 nic_qp_scheduling[0x1];
    u8 reserved_at_22[0x1e];

    u8 reserved_at_40[0xc0];

    u8 nic_element_type[0x10];
    u8 nic_tsar_type[0x10];

    u8 reserved_at_120[0x6e0];
};

struct mlx5_ifc_cmd_hca_cap_2_bits {
    u8 reserved_at_0[0x80];

    u8 reserved_at_80[0x13];
    u8 log_reserved_qpn_granularity[0x5];
    u8 reserved_at_98[0x8];

    u8 reserved_at_a0[0x760];
};

enum {
    MLX5_CRYPTO_CAPS_WRAPPED_IMPORT_METHOD_AES = 0x4,
};

struct mlx5_ifc_crypto_caps_bits {
    u8 wrapped_crypto_operational[0x1];
    u8 wrapped_crypto_going_to_commissioning[0x1];
    u8 reserved_at_2[0x16];
    u8 wrapped_import_method[0x8];

    u8 reserved_at_20[0xb];
    u8 log_max_num_deks[0x5];
    u8 reserved_at_30[0x3];
    u8 log_max_num_import_keks[0x5];
    u8 reserved_at_38[0x3];
    u8 log_max_num_creds[0x5];

    u8 failed_selftests[0x10];
    u8 num_nv_import_keks[0x8];
    u8 num_nv_credentials[0x8];

    u8 reserved_at_60[0x7a0];
};

union mlx5_ifc_hca_cap_union_bits {
    struct mlx5_ifc_atomic_caps_bits atomic_caps;
    struct mlx5_ifc_cmd_hca_cap_bits cmd_hca_cap;
    struct mlx5_ifc_flow_table_nic_cap_bits flow_table_nic_cap;
    struct mlx5_ifc_flow_table_eswitch_cap_bits flow_table_eswitch_cap;
    struct mlx5_ifc_e_switch_cap_bits e_switch_cap;
    struct mlx5_ifc_device_mem_cap_bits device_mem_cap;
    struct mlx5_ifc_odp_cap_bits odp_cap;
    struct mlx5_ifc_roce_cap_bits roce_caps;
    struct mlx5_ifc_qos_cap_bits qos_caps;
    struct mlx5_ifc_cmd_hca_cap_2_bits cmd_hca_cap_2;
    struct mlx5_ifc_crypto_caps_bits crypto_caps;
    u8 reserved_at_0[0x8000];
};

struct mlx5_ifc_query_hca_cap_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    union mlx5_ifc_hca_cap_union_bits capability;
};

struct mlx5_ifc_query_hca_cap_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_function[0x1];
    u8 reserved_at_41[0xf];
    u8 function_id[0x10];

    u8 reserved_at_60[0x20];
};

enum mlx5_cap_type {
    MLX5_CAP_GENERAL = 0,
    MLX5_CAP_ODP = 2,
    MLX5_CAP_ATOMIC = 3,
    MLX5_CAP_ROCE,
    MLX5_CAP_NUM,
};

enum {
    MLX5_SET_HCA_CAP_OP_MOD_GENERAL_DEVICE = 0x0 << 1,
    MLX5_SET_HCA_CAP_OP_MOD_ROCE = 0x4 << 1,
    MLX5_SET_HCA_CAP_OP_MOD_NIC_FLOW_TABLE = 0x7 << 1,
    MLX5_SET_HCA_CAP_OP_MOD_ESW_FLOW_TABLE = 0x8 << 1,
    MLX5_SET_HCA_CAP_OP_MOD_QOS = 0xc << 1,
    MLX5_SET_HCA_CAP_OP_MOD_ESW = 0x9 << 1,
    MLX5_SET_HCA_CAP_OP_MOD_DEVICE_MEMORY = 0xf << 1,
    MLX5_SET_HCA_CAP_OP_MOD_CRYPTO = 0x1a << 1,
    MLX5_SET_HCA_CAP_OP_MOD_GENERAL_DEVICE_CAP_2 = 0x20 << 1,
};

enum {
    MLX5_MKC_ACCESS_MODE_MTT = 0x1,
    MLX5_MKC_ACCESS_MODE_KLMS = 0x2,
};

struct mlx5_ifc_mkc_bits {
    u8 reserved_at_0[0x1];
    u8 free[0x1];
    u8 reserved_at_2[0x1];
    u8 access_mode_4_2[0x3];
    u8 reserved_at_6[0x7];
    u8 relaxed_ordering_write[0x1];
    u8 reserved_at_e[0x1];
    u8 small_fence_on_rdma_read_response[0x1];
    u8 umr_en[0x1];
    u8 a[0x1];
    u8 rw[0x1];
    u8 rr[0x1];
    u8 lw[0x1];
    u8 lr[0x1];
    u8 access_mode_1_0[0x2];
    u8 reserved_at_18[0x8];

    u8 qpn[0x18];
    u8 mkey_7_0[0x8];

    u8 reserved_at_40[0x20];

    u8 length64[0x1];
    u8 bsf_en[0x1];
    u8 sync_umr[0x1];
    u8 reserved_at_63[0x2];
    u8 expected_sigerr_count[0x1];
    u8 reserved_at_66[0x1];
    u8 en_rinval[0x1];
    u8 pd[0x18];

    u8 start_addr[0x40];

    u8 len[0x40];

    u8 bsf_octword_size[0x20];

    u8 reserved_at_120[0x80];

    u8 translations_octword_size[0x20];

    u8 reserved_at_1c0[0x19];
    u8 relaxed_ordering_read[0x1];
    u8 reserved_at_1d9[0x1];
    u8 log_page_size[0x5];

    u8 reserved_at_1e0[0x3];
    u8 crypto_en[0x2];
    u8 reserved_at_1e5[0x1b];
};

struct mlx5_ifc_create_mkey_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 mkey_index[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_mkey_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x20];

    u8 pg_access[0x1];
    u8 mkey_umem_valid[0x1];
    u8 reserved_at_62[0x1e];

    struct mlx5_ifc_mkc_bits memory_key_mkey_entry;

    u8 reserved_at_280[0x80];

    u8 translations_octword_actual_size[0x20];

    u8 reserved_at_320[0x560];

    u8 klm_pas_mtt[0][0x20];
};

struct mlx5_ifc_destroy_mkey_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_destroy_mkey_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 mkey_index[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_l2_hdr_bits {
    u8 dmac_47_16[0x20];
    u8 dmac_15_0[0x10];
    u8 smac_47_32[0x10];
    u8 smac_31_0[0x20];
    u8 ethertype[0x10];
    u8 vlan_type[0x10];
    u8 vlan[0x10];
};

enum {
    FS_FT_NIC_RX = 0x0,
    FS_FT_NIC_TX = 0x1,
    FS_FT_ESW_EGRESS_ACL = 0x2,
    FS_FT_ESW_INGRESS_ACL = 0x3,
    FS_FT_FDB = 0X4,
    FS_FT_SNIFFER_RX = 0X5,
    FS_FT_SNIFFER_TX = 0X6,
};

struct mlx5_ifc_ste_general_bits {
    u8 entry_type[0x4];
    u8 reserved_at_4[0x4];
    u8 entry_sub_type[0x8];
    u8 byte_mask[0x10];
    u8 next_table_base_63_48[0x10];
    u8 next_lu_type[0x8];
    u8 next_table_base_39_32_size[0x8];
    u8 next_table_base_31_5_size[0x1b];
    u8 linear_hash_enable[0x1];
    u8 reserved_at_5c[0x2];
    u8 next_table_rank[0x2];
    u8 reserved_at_60[0xa0];
    u8 tag_value[0x60];
    u8 bit_mask[0x60];
};

struct mlx5_ifc_ste_sx_transmit_bits {
    u8 entry_type[0x4];
    u8 reserved_at_4[0x4];
    u8 entry_sub_type[0x8];
    u8 byte_mask[0x10];

    u8 next_table_base_63_48[0x10];
    u8 next_lu_type[0x8];
    u8 next_table_base_39_32_size[0x8];

    u8 next_table_base_31_5_size[0x1b];
    u8 linear_hash_enable[0x1];
    u8 reserved_at_5c[0x2];
    u8 next_table_rank[0x2];

    u8 sx_wire[0x1];
    u8 sx_func_lb[0x1];
    u8 sx_sniffer[0x1];
    u8 sx_wire_enable[0x1];
    u8 sx_func_lb_enable[0x1];
    u8 sx_sniffer_enable[0x1];
    u8 action_type[0x3];
    u8 reserved_at_69[0x1];
    u8 action_description[0x6];
    u8 gvmi[0x10];

    u8 encap_pointer_vlan_data[0x20];

    u8 loopback_syndome_en[0x8];
    u8 loopback_syndome[0x8];
    u8 counter_trigger[0x10];

    u8 miss_address_63_48[0x10];
    u8 counter_trigger_23_16[0x8];
    u8 miss_address_39_32[0x8];

    u8 miss_address_31_6[0x1a];
    u8 learning_point[0x1];
    u8 go_back[0x1];
    u8 match_polarity[0x1];
    u8 mask_mode[0x1];
    u8 miss_rank[0x2];
};

struct mlx5_ifc_ste_rx_steering_mult_bits {
    u8 entry_type[0x4];
    u8 reserved_at_4[0x4];
    u8 entry_sub_type[0x8];
    u8 byte_mask[0x10];

    u8 next_table_base_63_48[0x10];
    u8 next_lu_type[0x8];
    u8 next_table_base_39_32_size[0x8];

    u8 next_table_base_31_5_size[0x1b];
    u8 linear_hash_enable[0x1];
    u8 reserved_at_5c[0x2];
    u8 next_table_rank[0x2];

    u8 member_count[0x10];
    u8 gvmi[0x10];

    u8 qp_list_pointer[0x20];

    u8 reserved_at_a0[0x1];
    u8 tunneling_action[0x3];
    u8 action_description[0x4];
    u8 reserved_at_a8[0x8];
    u8 counter_trigger_15_0[0x10];

    u8 miss_address_63_48[0x10];
    u8 counter_trigger_23_16[0x08];
    u8 miss_address_39_32[0x8];

    u8 miss_address_31_6[0x1a];
    u8 learning_point[0x1];
    u8 fail_on_error[0x1];
    u8 match_polarity[0x1];
    u8 mask_mode[0x1];
    u8 miss_rank[0x2];
};

struct mlx5_ifc_ste_modify_packet_bits {
    u8 entry_type[0x4];
    u8 reserved_at_4[0x4];
    u8 entry_sub_type[0x8];
    u8 byte_mask[0x10];

    u8 next_table_base_63_48[0x10];
    u8 next_lu_type[0x8];
    u8 next_table_base_39_32_size[0x8];

    u8 next_table_base_31_5_size[0x1b];
    u8 linear_hash_enable[0x1];
    u8 reserved_at_5c[0x2];
    u8 next_table_rank[0x2];

    u8 number_of_re_write_actions[0x10];
    u8 gvmi[0x10];

    u8 header_re_write_actions_pointer[0x20];

    u8 reserved_at_a0[0x1];
    u8 tunneling_action[0x3];
    u8 action_description[0x4];
    u8 reserved_at_a8[0x8];
    u8 counter_trigger_15_0[0x10];

    u8 miss_address_63_48[0x10];
    u8 counter_trigger_23_16[0x08];
    u8 miss_address_39_32[0x8];

    u8 miss_address_31_6[0x1a];
    u8 learning_point[0x1];
    u8 fail_on_error[0x1];
    u8 match_polarity[0x1];
    u8 mask_mode[0x1];
    u8 miss_rank[0x2];
};

struct mlx5_ifc_ste_single_action_flow_tag_v1_bits {
    u8 action_id[0x8];
    u8 flow_tag[0x18];
};

struct mlx5_ifc_ste_single_action_modify_list_v1_bits {
    u8 action_id[0x8];
    u8 num_of_modify_actions[0x8];
    u8 modify_actions_ptr[0x10];
};

struct mlx5_ifc_ste_single_action_remove_header_v1_bits {
    u8 action_id[0x8];
    u8 reserved_at_8[0x2];
    u8 start_anchor[0x6];
    u8 reserved_at_10[0x2];
    u8 end_anchor[0x6];
    u8 reserved_at_18[0x4];
    u8 decap[0x1];
    u8 vni_to_cqe[0x1];
    u8 qos_profile[0x2];
};

struct mlx5_ifc_ste_single_action_remove_header_size_v1_bits {
    u8 action_id[0x8];
    u8 reserved_at_8[0x2];
    u8 start_anchor[0x6];
    u8 outer_l4_remove[0x1];
    u8 reserved_at_11[0x1];
    u8 start_offset[0x7];
    u8 reserved_at_18[0x1];
    u8 remove_size[0x6];
};

struct mlx5_ifc_ste_double_action_copy_v1_bits {
    u8 action_id[0x8];
    u8 destination_dw_offset[0x8];
    u8 reserved_at_10[0x2];
    u8 destination_left_shifter[0x6];
    u8 reserved_at_18[0x2];
    u8 destination_length[0x6];

    u8 reserved_at_20[0x8];
    u8 source_dw_offset[0x8];
    u8 reserved_at_30[0x2];
    u8 source_right_shifter[0x6];
    u8 reserved_at_38[0x8];
};

struct mlx5_ifc_ste_double_action_set_v1_bits {
    u8 action_id[0x8];
    u8 destination_dw_offset[0x8];
    u8 reserved_at_10[0x2];
    u8 destination_left_shifter[0x6];
    u8 reserved_at_18[0x2];
    u8 destination_length[0x6];

    u8 inline_data[0x20];
};

struct mlx5_ifc_ste_double_action_add_v1_bits {
    u8 action_id[0x8];
    u8 destination_dw_offset[0x8];
    u8 reserved_at_10[0x2];
    u8 destination_left_shifter[0x6];
    u8 reserved_at_18[0x2];
    u8 destination_length[0x6];

    u8 add_value[0x20];
};

struct mlx5_ifc_ste_double_action_insert_with_inline_v1_bits {
    u8 action_id[0x8];
    u8 reserved_at_8[0x2];
    u8 start_anchor[0x6];
    u8 start_offset[0x7];
    u8 reserved_at_17[0x9];

    u8 inline_data[0x20];
};

struct mlx5_ifc_ste_double_action_insert_with_ptr_v1_bits {
    u8 action_id[0x8];
    u8 reserved_at_8[0x2];
    u8 start_anchor[0x6];
    u8 start_offset[0x7];
    u8 size[0x6];
    u8 attributes[0x3];

    u8 pointer[0x20];
};

struct mlx5_ifc_ste_double_action_modify_action_list_v1_bits {
    u8 action_id[0x8];
    u8 modify_actions_pattern_pointer[0x18];

    u8 number_of_modify_actions[0x8];
    u8 modify_actions_argument_pointer[0x18];
};

enum {
    MLX5_IFC_ASO_FLOW_METER_INITIAL_COLOR_RED = 0x0,
    MLX5_IFC_ASO_FLOW_METER_INITIAL_COLOR_YELLOW = 0x1,
    MLX5_IFC_ASO_FLOW_METER_INITIAL_COLOR_GREEN = 0x2,
    MLX5_IFC_ASO_FLOW_METER_INITIAL_COLOR_UNDEFINED = 0x3,
};

enum {
    MLX5_IFC_ASO_CT_DIRECTION_INITIATOR = 0x0,
    MLX5_IFC_ASO_CT_DIRECTION_RESPONDER = 0x1,
};

struct mlx5_ifc_ste_aso_first_hit_action_v1_bits {
    u8 reserved_at_0[0x6];
    u8 set[0x1];
    u8 line_id[0x9];
};

struct mlx5_ifc_ste_aso_flow_meter_action_v1_bits {
    u8 reserved_at_0[0xc];
    u8 action[0x1];
    u8 initial_color[0x2];
    u8 line_id[0x1];
};

struct mlx5_ifc_ste_aso_ct_action_v1_bits {
    u8 reserved_at_0[0xf];
    u8 direction[0x1];
};

struct mlx5_ifc_ste_double_action_aso_v1_bits {
    u8 action_id[0x8];
    u8 aso_context_number[0x18];

    u8 dest_reg_id[0x2];
    u8 change_ordering_tag[0x1];
    u8 aso_check_ordering[0x1];
    u8 aso_context_type[0x4];
    u8 reserved_at_28[0x8];
    union {
        u8 aso_fields[0x10];
        struct mlx5_ifc_ste_aso_first_hit_action_v1_bits first_hit;
        struct mlx5_ifc_ste_aso_flow_meter_action_v1_bits flow_meter;
        struct mlx5_ifc_ste_aso_ct_action_v1_bits ct;
    };
};

struct mlx5_ifc_ste_match_bwc_v1_bits {
    u8 entry_format[0x8];
    u8 counter_id[0x18];

    u8 miss_address_63_48[0x10];
    u8 match_definer_ctx_idx[0x8];
    u8 miss_address_39_32[0x8];

    u8 miss_address_31_6[0x1a];
    u8 reserved_at_5a[0x1];
    u8 match_polarity[0x1];
    u8 reparse[0x1];
    u8 reserved_at_5d[0x3];

    u8 next_table_base_63_48[0x10];
    u8 hash_definer_ctx_idx[0x8];
    u8 next_table_base_39_32_size[0x8];

    u8 next_table_base_31_5_size[0x1b];
    u8 hash_type[0x2];
    u8 hash_after_actions[0x1];
    u8 reserved_at_9e[0x2];

    u8 byte_mask[0x10];
    u8 next_entry_format[0x1];
    u8 mask_mode[0x1];
    u8 gvmi[0xe];

    u8 action[0x40];
};

struct mlx5_ifc_ste_mask_and_match_v1_bits {
    u8 entry_format[0x8];
    u8 counter_id[0x18];

    u8 miss_address_63_48[0x10];
    u8 match_definer_ctx_idx[0x8];
    u8 miss_address_39_32[0x8];

    u8 miss_address_31_6[0x1a];
    u8 reserved_at_5a[0x1];
    u8 match_polarity[0x1];
    u8 reparse[0x1];
    u8 reserved_at_5d[0x3];

    u8 next_table_base_63_48[0x10];
    u8 hash_definer_ctx_idx[0x8];
    u8 next_table_base_39_32_size[0x8];

    u8 next_table_base_31_5_size[0x1b];
    u8 hash_type[0x2];
    u8 hash_after_actions[0x1];
    u8 reserved_at_9e[0x2];

    u8 action[0x60];
};

struct mlx5_ifc_ste_eth_l2_src_bits {
    u8 smac_47_16[0x20];

    u8 smac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 qp_type[0x2];
    u8 ethertype_filter[0x1];
    u8 reserved_at_43[0x1];
    u8 sx_sniffer[0x1];
    u8 force_lb[0x1];
    u8 functional_lb[0x1];
    u8 port[0x1];
    u8 reserved_at_48[0x4];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_qualifier[0x2];
    u8 reserved_at_52[0x2];
    u8 first_vlan_id[0xc];

    u8 ip_fragmented[0x1];
    u8 tcp_syn[0x1];
    u8 encp_type[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 reserved_at_68[0x4];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_qualifier[0x2];
    u8 reserved_at_72[0x2];
    u8 second_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_src_v1_bits {
    u8 reserved_at_0[0x1];
    u8 sx_sniffer[0x1];
    u8 functional_loopback[0x1];
    u8 ip_fragmented[0x1];
    u8 qp_type[0x2];
    u8 encapsulation_type[0x2];
    u8 port[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 first_vlan_qualifier[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];

    u8 smac_47_16[0x20];

    u8 smac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 reserved_at_60[0x6];
    u8 tcp_syn[0x1];
    u8 reserved_at_67[0x3];
    u8 force_loopback[0x1];
    u8 l2_ok[0x1];
    u8 l3_ok[0x1];
    u8 l4_ok[0x1];
    u8 second_vlan_qualifier[0x2];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_dst_bits {
    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 qp_type[0x2];
    u8 ethertype_filter[0x1];
    u8 reserved_at_43[0x1];
    u8 sx_sniffer[0x1];
    u8 force_lb[0x1];
    u8 functional_lb[0x1];
    u8 port[0x1];
    u8 reserved_at_48[0x4];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_qualifier[0x2];
    u8 reserved_at_52[0x2];
    u8 first_vlan_id[0xc];

    u8 ip_fragmented[0x1];
    u8 tcp_syn[0x1];
    u8 encp_type[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 reserved_at_68[0x4];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_qualifier[0x2];
    u8 reserved_at_72[0x2];
    u8 second_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_dst_v1_bits {
    u8 reserved_at_0[0x1];
    u8 sx_sniffer[0x1];
    u8 functional_lb[0x1];
    u8 ip_fragmented[0x1];
    u8 qp_type[0x2];
    u8 encapsulation_type[0x2];
    u8 port[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 first_vlan_qualifier[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];

    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 reserved_at_60[0x6];
    u8 tcp_syn[0x1];
    u8 reserved_at_67[0x3];
    u8 force_lb[0x1];
    u8 l2_ok[0x1];
    u8 l3_ok[0x1];
    u8 l4_ok[0x1];
    u8 second_vlan_qualifier[0x2];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_src_dst_bits {
    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 smac_47_32[0x10];

    u8 smac_31_0[0x20];

    u8 sx_sniffer[0x1];
    u8 force_lb[0x1];
    u8 functional_lb[0x1];
    u8 port[0x1];
    u8 l3_type[0x2];
    u8 reserved_at_66[0x6];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_qualifier[0x2];
    u8 reserved_at_72[0x2];
    u8 first_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_src_dst_v1_bits {
    u8 dmac_47_16[0x20];

    u8 smac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 reserved_at_50[0x2];
    u8 functional_lb[0x1];
    u8 reserved_at_53[0x5];
    u8 port[0x2];
    u8 l3_type[0x2];
    u8 reserved_at_5c[0x2];
    u8 first_vlan_qualifier[0x2];

    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];
    u8 smac_15_0[0x10];
};

struct mlx5_ifc_ste_eth_l3_ipv4_5_tuple_bits {
    u8 destination_address[0x20];

    u8 source_address[0x20];

    u8 source_port[0x10];
    u8 destination_port[0x10];

    u8 fragmented[0x1];
    u8 first_fragment[0x1];
    u8 reserved_at_62[0x2];
    u8 reserved_at_64[0x1];
    u8 ecn[0x2];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 dscp[0x6];
    u8 reserved_at_76[0x2];
    u8 protocol[0x8];
};

struct mlx5_ifc_ste_eth_l3_ipv4_5_tuple_v1_bits {
    u8 source_address[0x20];

    u8 destination_address[0x20];

    u8 source_port[0x10];
    u8 destination_port[0x10];

    u8 reserved_at_60[0x4];
    u8 l4_ok[0x1];
    u8 l3_ok[0x1];
    u8 fragmented[0x1];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 dscp[0x6];
    u8 ecn[0x2];
    u8 protocol[0x8];
};

struct mlx5_ifc_ste_eth_l3_ipv6_dst_bits {
    u8 dst_ip_127_96[0x20];

    u8 dst_ip_95_64[0x20];

    u8 dst_ip_63_32[0x20];

    u8 dst_ip_31_0[0x20];
};

struct mlx5_ifc_ste_eth_l2_tnl_bits {
    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 l2_tunneling_network_id[0x20];

    u8 ip_fragmented[0x1];
    u8 tcp_syn[0x1];
    u8 encp_type[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 reserved_at_6c[0x3];
    u8 gre_key_flag[0x1];
    u8 first_vlan_qualifier[0x2];
    u8 reserved_at_72[0x2];
    u8 first_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l2_tnl_v1_bits {
    u8 l2_tunneling_network_id[0x20];

    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 l3_ethertype[0x10];

    u8 reserved_at_60[0x3];
    u8 ip_fragmented[0x1];
    u8 reserved_at_64[0x2];
    u8 encp_type[0x2];
    u8 reserved_at_68[0x2];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 first_vlan_qualifier[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];
};

struct mlx5_ifc_ste_eth_l3_ipv6_src_bits {
    u8 src_ip_127_96[0x20];

    u8 src_ip_95_64[0x20];

    u8 src_ip_63_32[0x20];

    u8 src_ip_31_0[0x20];
};

struct mlx5_ifc_ste_eth_l3_ipv4_misc_bits {
    u8 version[0x4];
    u8 ihl[0x4];
    u8 reserved_at_8[0x8];
    u8 total_length[0x10];

    u8 identification[0x10];
    u8 flags[0x3];
    u8 fragment_offset[0xd];

    u8 time_to_live[0x8];
    u8 reserved_at_48[0x8];
    u8 checksum[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_eth_l3_ipv4_misc_v1_bits {
    u8 identification[0x10];
    u8 flags[0x3];
    u8 fragment_offset[0xd];

    u8 total_length[0x10];
    u8 checksum[0x10];

    u8 version[0x4];
    u8 ihl[0x4];
    u8 time_to_live[0x8];
    u8 reserved_at_50[0x10];

    u8 reserved_at_60[0x1c];
    u8 voq_internal_prio[0x4];
};

struct mlx5_ifc_ste_eth_l4_bits {
    u8 fragmented[0x1];
    u8 first_fragment[0x1];
    u8 reserved_at_2[0x6];
    u8 protocol[0x8];
    u8 dst_port[0x10];

    u8 ipv6_version[0x4];
    u8 reserved_at_24[0x1];
    u8 ecn[0x2];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 src_port[0x10];

    u8 ipv6_payload_length[0x10];
    u8 ipv6_hop_limit[0x8];
    u8 dscp[0x6];
    u8 reserved_at_5e[0x2];

    u8 tcp_data_offset[0x4];
    u8 reserved_at_64[0x8];
    u8 flow_label[0x14];
};

struct mlx5_ifc_ste_eth_l4_v1_bits {
    u8 ipv6_version[0x4];
    u8 reserved_at_4[0x4];
    u8 dscp[0x6];
    u8 ecn[0x2];
    u8 ipv6_hop_limit[0x8];
    u8 protocol[0x8];

    u8 src_port[0x10];
    u8 dst_port[0x10];

    u8 first_fragment[0x1];
    u8 reserved_at_41[0xb];
    u8 flow_label[0x14];

    u8 tcp_data_offset[0x4];
    u8 l4_ok[0x1];
    u8 l3_ok[0x1];
    u8 fragmented[0x1];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 ipv6_paylen[0x10];
};

struct mlx5_ifc_ste_eth_l4_misc_bits {
    u8 checksum[0x10];
    u8 length[0x10];

    u8 seq_num[0x20];

    u8 ack_num[0x20];

    u8 urgent_pointer[0x10];
    u8 window_size[0x10];
};

struct mlx5_ifc_ste_eth_l4_misc_v1_bits {
    u8 window_size[0x10];
    u8 urgent_pointer[0x10];

    u8 ack_num[0x20];

    u8 seq_num[0x20];

    u8 length[0x10];
    u8 checksum[0x10];
};

struct mlx5_ifc_ste_mpls_bits {
    u8 mpls0_label[0x14];
    u8 mpls0_exp[0x3];
    u8 mpls0_s_bos[0x1];
    u8 mpls0_ttl[0x8];

    u8 mpls1_label[0x20];

    u8 mpls2_label[0x20];

    u8 reserved_at_60[0x16];
    u8 mpls4_s_bit[0x1];
    u8 mpls4_qualifier[0x1];
    u8 mpls3_s_bit[0x1];
    u8 mpls3_qualifier[0x1];
    u8 mpls2_s_bit[0x1];
    u8 mpls2_qualifier[0x1];
    u8 mpls1_s_bit[0x1];
    u8 mpls1_qualifier[0x1];
    u8 mpls0_s_bit[0x1];
    u8 mpls0_qualifier[0x1];
};

struct mlx5_ifc_ste_mpls_v1_bits {
    u8 reserved_at_0[0x15];
    u8 mpls_ok[0x1];
    u8 mpls4_s_bit[0x1];
    u8 mpls4_qualifier[0x1];
    u8 mpls3_s_bit[0x1];
    u8 mpls3_qualifier[0x1];
    u8 mpls2_s_bit[0x1];
    u8 mpls2_qualifier[0x1];
    u8 mpls1_s_bit[0x1];
    u8 mpls1_qualifier[0x1];
    u8 mpls0_s_bit[0x1];
    u8 mpls0_qualifier[0x1];

    u8 mpls0_label[0x14];
    u8 mpls0_exp[0x3];
    u8 mpls0_s_bos[0x1];
    u8 mpls0_ttl[0x8];

    u8 mpls1_label[0x20];

    u8 mpls2_label[0x20];
};

struct mlx5_ifc_ste_register_0_bits {
    u8 register_0_h[0x20];

    u8 register_0_l[0x20];

    u8 register_1_h[0x20];

    u8 register_1_l[0x20];
};

struct mlx5_ifc_ste_register_1_bits {
    u8 register_2_h[0x20];

    u8 register_2_l[0x20];

    u8 register_3_h[0x20];

    u8 register_3_l[0x20];
};

struct mlx5_ifc_ste_gre_bits {
    u8 gre_c_present[0x1];
    u8 reserved_at_1[0x1];
    u8 gre_k_present[0x1];
    u8 gre_s_present[0x1];
    u8 strict_src_route[0x1];
    u8 recur[0x3];
    u8 flags[0x5];
    u8 version[0x3];
    u8 gre_protocol[0x10];

    u8 checksum[0x10];
    u8 offset[0x10];

    u8 gre_key_h[0x18];
    u8 gre_key_l[0x8];

    u8 seq_num[0x20];
};

struct mlx5_ifc_ste_gre_v1_bits {
    u8 gre_c_present[0x1];
    u8 reserved_at_1[0x1];
    u8 gre_k_present[0x1];
    u8 gre_s_present[0x1];
    u8 strict_src_route[0x1];
    u8 recur[0x3];
    u8 flags[0x5];
    u8 version[0x3];
    u8 gre_protocol[0x10];

    u8 reserved_at_20[0x20];

    u8 gre_key_h[0x18];
    u8 gre_key_l[0x8];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_flex_parser_0_bits {
    u8 flex_parser_3[0x20];

    u8 flex_parser_2[0x20];

    u8 flex_parser_1[0x20];

    u8 flex_parser_0[0x20];
};

struct mlx5_ifc_ste_flex_parser_1_bits {
    u8 flex_parser_7[0x20];

    u8 flex_parser_6[0x20];

    u8 flex_parser_5[0x20];

    u8 flex_parser_4[0x20];
};

struct mlx5_ifc_ste_tunnel_header_bits {
    u8 tunnel_header_dw0[0x20];

    u8 tunnel_header_dw1[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_ste_tunnel_header_v1_bits {
    u8 tunnel_header_0[0x20];

    u8 tunnel_header_1[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_ste_flex_parser_tnl_vxlan_gpe_bits {
    u8 outer_vxlan_gpe_flags[0x8];
    u8 reserved_at_8[0x10];
    u8 outer_vxlan_gpe_next_protocol[0x8];

    u8 outer_vxlan_gpe_vni[0x18];
    u8 reserved_at_38[0x8];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_ste_flex_parser_tnl_geneve_bits {
    u8 reserved_at_0[0x2];
    u8 geneve_opt_len[0x6];
    u8 geneve_oam[0x1];
    u8 reserved_at_9[0x7];
    u8 geneve_protocol_type[0x10];

    u8 geneve_vni[0x18];
    u8 reserved_at_38[0x8];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_ste_flex_parser_tnl_gtpu_bits {
    u8 gtpu_msg_flags[0x8];
    u8 gtpu_msg_type[0x8];
    u8 reserved_at_10[0x10];

    u8 gtpu_teid[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_ste_general_purpose_bits {
    u8 general_purpose_lookup_field[0x20];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_src_gvmi_qp_bits {
    u8 loopback_syndrome[0x8];
    u8 reserved_at_8[0x8];
    u8 source_gvmi[0x10];

    u8 reserved_at_20[0x5];
    u8 force_lb[0x1];
    u8 functional_lb[0x1];
    u8 source_is_requestor[0x1];
    u8 source_qp[0x18];

    u8 reserved_at_40[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_src_gvmi_qp_v1_bits {
    u8 loopback_synd[0x8];
    u8 reserved_at_8[0x7];
    u8 functional_lb[0x1];
    u8 source_gvmi[0x10];

    u8 force_lb[0x1];
    u8 reserved_at_21[0x1];
    u8 source_is_requestor[0x1];
    u8 reserved_at_23[0x5];
    u8 source_qp[0x18];

    u8 reserved_at_40[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_icmp_v1_bits {
    u8 icmp_payload_data[0x20];

    u8 icmp_header_data[0x20];

    u8 icmp_type[0x8];
    u8 icmp_code[0x8];
    u8 reserved_at_50[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_ste_def0_v1_bits {
    u8 metadata_reg_c_0[0x20];

    u8 metadata_reg_c_1[0x20];

    u8 dmac_47_16[0x20];

    u8 dmac_15_0[0x10];
    u8 ethertype[0x10];

    u8 reserved_at_60[0x1];
    u8 sx_sniffer[0x1];
    u8 functional_loopback[0x1];
    u8 ip_frag[0x1];
    u8 qp_type[0x2];
    u8 encapsulation_type[0x2];
    u8 port[0x2];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 first_vlan_qualifier[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];

    u8 reserved_at_80[0xa];
    u8 force_loopback[0x1];
    u8 reserved_at_8b[0x3];
    u8 second_vlan_qualifier[0x2];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_id[0xc];

    u8 smac_47_16[0x20];

    u8 smac_15_0[0x10];
    u8 inner_ipv4_checksum_ok[0x1];
    u8 inner_l4_checksum_ok[0x1];
    u8 outer_ipv4_checksum_ok[0x1];
    u8 outer_l4_checksum_ok[0x1];
    u8 inner_l3_ok[0x1];
    u8 inner_l4_ok[0x1];
    u8 outer_l3_ok[0x1];
    u8 outer_l4_ok[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
};

struct mlx5_ifc_ste_def2_v1_bits {
    u8 metadata_reg_a[0x20];

    u8 outer_ip_version[0x4];
    u8 outer_ip_ihl[0x4];
    u8 outer_ip_dscp[0x6];
    u8 outer_ip_ecn[0x2];
    u8 outer_ip_ttl[0x8];
    u8 outer_ip_protocol[0x8];

    u8 outer_ip_identification[0x10];
    u8 outer_ip_flags[0x3];
    u8 outer_ip_fragment_offset[0xd];

    u8 outer_ip_total_length[0x10];
    u8 outer_ip_checksum[0x10];

    u8 reserved_180[0xc];
    u8 outer_ip_flow_label[0x14];

    u8 outer_eth_packet_length[0x10];
    u8 outer_ip_payload_length[0x10];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 outer_data_offset[0x4];
    u8 reserved_1e4[0x2];
    u8 outer_ip_frag[0x1];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 outer_ip_frag_first[0x1];
    u8 reserved_1f0[0x7];
    u8 inner_ipv4_checksum_ok[0x1];
    u8 inner_l4_checksum_ok[0x1];
    u8 outer_ipv4_checksum_ok[0x1];
    u8 outer_l4_checksum_ok[0x1];
    u8 inner_l3_ok[0x1];
    u8 inner_l4_ok[0x1];
    u8 outer_l3_ok[0x1];
    u8 outer_l4_ok[0x1];
};

struct mlx5_ifc_ste_def6_v1_bits {
    u8 dst_ipv6_127_96[0x20];

    u8 dst_ipv6_95_64[0x20];

    u8 dst_ipv6_63_32[0x20];

    u8 dst_ipv6_31_0[0x20];

    u8 reserved_at_80[0x40];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 reserved_e0[0x4];
    u8 l4_ok[0x1];
    u8 l3_ok[0x1];
    u8 ip_frag[0x1];
    u8 tcp_ns[0x1];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
    u8 reserved_f0[0x10];
};

struct mlx5_ifc_ste_def16_v1_bits {
    u8 tunnel_header_0[0x20];

    u8 tunnel_header_1[0x20];

    u8 tunnel_header_2[0x20];

    u8 tunnel_header_3[0x20];

    u8 random_number[0x10];
    u8 reserved_90[0x10];

    u8 metadata_reg_a[0x20];

    u8 reserved_c0[0x8];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 outer_first_vlan_type[0x2];
    u8 reserved_ce[0x1];
    u8 functional_lb[0x1];
    u8 source_gvmi[0x10];

    u8 force_lb[0x1];
    u8 outer_ip_frag[0x1];
    u8 source_is_requester[0x1];
    u8 reserved_e3[0x5];
    u8 source_sqn[0x18];
};

struct mlx5_ifc_ste_def22_v1_bits {
    u8 outer_ip_src_addr[0x20];

    u8 outer_ip_dst_addr[0x20];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 reserved_at_40[0x1];
    u8 sx_sniffer[0x1];
    u8 functional_loopback[0x1];
    u8 outer_ip_frag[0x1];
    u8 qp_type[0x2];
    u8 encapsulation_type[0x2];
    u8 port[0x2];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 first_vlan_qualifier[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];

    u8 metadata_reg_c_0[0x20];

    u8 outer_dmac_47_16[0x20];

    u8 outer_smac_47_16[0x20];

    u8 outer_smac_15_0[0x10];
    u8 outer_dmac_15_0[0x10];
};

struct mlx5_ifc_ste_def24_v1_bits {
    u8 metadata_reg_c_2[0x20];

    u8 metadata_reg_c_3[0x20];

    u8 metadata_reg_c_0[0x20];

    u8 metadata_reg_c_1[0x20];

    u8 outer_ip_src_addr[0x20];

    u8 outer_ip_dst_addr[0x20];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 inner_ip_protocol[0x8];
    u8 inner_l3_type[0x2];
    u8 inner_l4_type[0x2];
    u8 inner_first_vlan_type[0x2];
    u8 inner_ip_frag[0x1];
    u8 functional_lb[0x1];
    u8 outer_ip_protocol[0x8];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 outer_first_vlan_type[0x2];
    u8 outer_ip_frag[0x1];
    u8 functional_lb_dup[0x1];
};

struct mlx5_ifc_ste_def25_v1_bits {
    u8 inner_ip_src_addr[0x20];

    u8 inner_ip_dst_addr[0x20];

    u8 inner_l4_sport[0x10];
    u8 inner_l4_dport[0x10];

    u8 tunnel_header_0[0x20];

    u8 tunnel_header_1[0x20];

    u8 reserved_at_a0[0x20];

    u8 port_number_dup[0x2];
    u8 inner_l3_type[0x2];
    u8 inner_l4_type[0x2];
    u8 inner_first_vlan_type[0x2];
    u8 port_number[0x2];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 outer_first_vlan_type[0x2];
    u8 outer_l4_dport[0x10];

    u8 reserved_at_e0[0x20];
};

struct mlx5_ifc_ste_def26_v1_bits {
    u8 src_ipv6_127_96[0x20];

    u8 src_ipv6_95_64[0x20];

    u8 src_ipv6_63_32[0x20];

    u8 src_ipv6_31_0[0x20];

    u8 reserved_at_80[0x3];
    u8 ip_frag[0x1];
    u8 reserved_at_84[0x6];
    u8 l3_type[0x2];
    u8 l4_type[0x2];
    u8 first_vlan_type[0x2];
    u8 first_priority[0x3];
    u8 first_cfi[0x1];
    u8 first_vlan_id[0xc];

    u8 reserved_at_a0[0xb];
    u8 l2_ok[0x1];
    u8 l3_ok[0x1];
    u8 l4_ok[0x1];
    u8 second_vlan_type[0x2];
    u8 second_priority[0x3];
    u8 second_cfi[0x1];
    u8 second_vlan_id[0xc];

    u8 smac_47_16[0x20];

    u8 smac_15_0[0x10];
    u8 ip_porotcol[0x8];
    u8 tcp_cwr[0x1];
    u8 tcp_ece[0x1];
    u8 tcp_urg[0x1];
    u8 tcp_ack[0x1];
    u8 tcp_psh[0x1];
    u8 tcp_rst[0x1];
    u8 tcp_syn[0x1];
    u8 tcp_fin[0x1];
};

struct mlx5_ifc_ste_def28_v1_bits {
    u8 inner_l4_sport[0x10];
    u8 inner_l4_dport[0x10];

    u8 flex_gtpu_teid[0x20];

    u8 inner_ip_src_addr[0x20];

    u8 inner_ip_dst_addr[0x20];

    u8 outer_ip_src_addr[0x20];

    u8 outer_ip_dst_addr[0x20];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 inner_ip_protocol[0x8];
    u8 inner_l3_type[0x2];
    u8 inner_l4_type[0x2];
    u8 inner_first_vlan_type[0x2];
    u8 inner_ip_frag[0x1];
    u8 functional_lb[0x1];
    u8 outer_ip_protocol[0x8];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 outer_first_vlan_type[0x2];
    u8 outer_ip_frag[0x1];
    u8 functional_lb_dup[0x1];
};

struct mlx5_ifc_ste_def33_v1_bits {
    u8 outer_ip_src_addr[0x20];

    u8 outer_ip_dst_addr[0x20];

    u8 outer_l4_sport[0x10];
    u8 outer_l4_dport[0x10];

    u8 reserved_at_60[0x1];
    u8 sx_sniffer[0x1];
    u8 functional_loopback[0x1];
    u8 outer_ip_frag[0x1];
    u8 qp_type[0x2];
    u8 encapsulation_type[0x2];
    u8 port[0x2];
    u8 outer_l3_type[0x2];
    u8 outer_l4_type[0x2];
    u8 outer_first_vlan_type[0x2];
    u8 outer_first_vlan_prio[0x3];
    u8 outer_first_vlan_cfi[0x1];
    u8 outer_first_vlan_vid[0xc];

    u8 reserved_at_80[0x20];

    u8 reserved_at_a0[0x20];

    u8 reserved_at_c0[0x20];

    u8 outer_ip_version[0x4];
    u8 outer_ip_ihl[0x4];
    u8 inner_ipv4_checksum_ok[0x1];
    u8 inner_l4_checksum_ok[0x1];
    u8 outer_ipv4_checksum_ok[0x1];
    u8 outer_l4_checksum_ok[0x1];
    u8 inner_l3_ok[0x1];
    u8 inner_l4_ok[0x1];
    u8 outer_l3_ok[0x1];
    u8 outer_l4_ok[0x1];
    u8 outer_ip_ttl[0x8];
    u8 outer_ip_protocol[0x8];
};

struct mlx5_ifc_set_action_in_bits {
    u8 action_type[0x4];
    u8 field[0xc];
    u8 reserved_at_10[0x3];
    u8 offset[0x5];
    u8 reserved_at_18[0x3];
    u8 length[0x5];

    u8 data[0x20];
};

struct mlx5_ifc_add_action_in_bits {
    u8 action_type[0x4];
    u8 field[0xc];
    u8 reserved_at_10[0x10];

    u8 data[0x20];
};

struct mlx5_ifc_copy_action_in_bits {
    u8 action_type[0x4];
    u8 src_field[0xc];
    u8 reserved_at_10[0x3];
    u8 src_offset[0x5];
    u8 reserved_at_18[0x3];
    u8 length[0x5];

    u8 reserved_at_20[0x4];
    u8 dst_field[0xc];
    u8 reserved_at_30[0x3];
    u8 dst_offset[0x5];
    u8 reserved_at_38[0x8];
};

enum {
    MLX5_ACTION_TYPE_SET = 0x1,
    MLX5_ACTION_TYPE_ADD = 0x2,
    MLX5_ACTION_TYPE_COPY = 0x3,
};

enum {
    MLX5_ACTION_IN_FIELD_OUT_SMAC_47_16 = 0x1,
    MLX5_ACTION_IN_FIELD_OUT_SMAC_15_0 = 0x2,
    MLX5_ACTION_IN_FIELD_OUT_ETHERTYPE = 0x3,
    MLX5_ACTION_IN_FIELD_OUT_DMAC_47_16 = 0x4,
    MLX5_ACTION_IN_FIELD_OUT_DMAC_15_0 = 0x5,
    MLX5_ACTION_IN_FIELD_OUT_IP_DSCP = 0x6,
    MLX5_ACTION_IN_FIELD_OUT_TCP_FLAGS = 0x7,
    MLX5_ACTION_IN_FIELD_OUT_TCP_SPORT = 0x8,
    MLX5_ACTION_IN_FIELD_OUT_TCP_DPORT = 0x9,
    MLX5_ACTION_IN_FIELD_OUT_IP_TTL = 0xa,
    MLX5_ACTION_IN_FIELD_OUT_UDP_SPORT = 0xb,
    MLX5_ACTION_IN_FIELD_OUT_UDP_DPORT = 0xc,
    MLX5_ACTION_IN_FIELD_OUT_SIPV6_127_96 = 0xd,
    MLX5_ACTION_IN_FIELD_OUT_SIPV6_95_64 = 0xe,
    MLX5_ACTION_IN_FIELD_OUT_SIPV6_63_32 = 0xf,
    MLX5_ACTION_IN_FIELD_OUT_SIPV6_31_0 = 0x10,
    MLX5_ACTION_IN_FIELD_OUT_DIPV6_127_96 = 0x11,
    MLX5_ACTION_IN_FIELD_OUT_DIPV6_95_64 = 0x12,
    MLX5_ACTION_IN_FIELD_OUT_DIPV6_63_32 = 0x13,
    MLX5_ACTION_IN_FIELD_OUT_DIPV6_31_0 = 0x14,
    MLX5_ACTION_IN_FIELD_OUT_SIPV4 = 0x15,
    MLX5_ACTION_IN_FIELD_OUT_DIPV4 = 0x16,
    MLX5_ACTION_IN_FIELD_OUT_FIRST_VID = 0x17,
    MLX5_ACTION_IN_FIELD_OUT_IPV6_HOPLIMIT = 0x47,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGA = 0x49,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGB = 0x50,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_0 = 0x51,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_1 = 0x52,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_2 = 0x53,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_3 = 0x54,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_4 = 0x55,
    MLX5_ACTION_IN_FIELD_OUT_METADATA_REGC_5 = 0x56,
    MLX5_ACTION_IN_FIELD_OUT_TCP_SEQ_NUM = 0x59,
    MLX5_ACTION_IN_FIELD_OUT_TCP_ACK_NUM = 0x5B,
    MLX5_ACTION_IN_FIELD_OUT_GTPU_TEID = 0x6E,
};

struct mlx5_ifc_dctc_bits {
    u8 reserved_at_0[0x1d];
    u8 data_in_order[0x1];
    u8 reserved_at_1e[0x362];
};

struct mlx5_ifc_packet_reformat_context_in_bits {
    u8 reserved_at_0[0x5];
    u8 reformat_type[0x3];
    u8 reserved_at_8[0xe];
    u8 reformat_data_size[0xa];

    u8 reserved_at_20[0x10];
    u8 reformat_data[2][0x8];

    u8 more_reformat_data[0][0x8];
};

struct mlx5_ifc_alloc_packet_reformat_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0xa0];

    struct mlx5_ifc_packet_reformat_context_in_bits packet_reformat_context;
};

struct mlx5_ifc_alloc_packet_reformat_context_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 packet_reformat_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_packet_reformat_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_20[0x10];
    u8 op_mod[0x10];

    u8 packet_reformat_id[0x20];

    u8 reserved_60[0x20];
};

struct mlx5_ifc_dealloc_packet_reformat_context_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

enum reformat_type {
    MLX5_REFORMAT_TYPE_L2_TO_VXLAN = 0x0,
    MLX5_REFORMAT_TYPE_L2_TO_NVGRE = 0x1,
    MLX5_REFORMAT_TYPE_L2_TO_L2_TUNNEL = 0x2,
    MLX5_REFORMAT_TYPE_L3_TUNNEL_TO_L2 = 0x3,
    MLX5_REFORMAT_TYPE_L2_TO_L3_TUNNEL = 0x4,
};

struct mlx5_ifc_alloc_flow_counter_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_alloc_flow_counter_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 flow_counter_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_flow_counter_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 flow_counter_id[0x20];

    u8 reserved_at_60[0x20];
};

enum {
    MLX5_OBJ_TYPE_FLOW_METER = 0x000a,
    MLX5_OBJ_TYPE_DEK = 0x000C,
    MLX5_OBJ_TYPE_MATCH_DEFINER = 0x0018,
    MLX5_OBJ_TYPE_CRYPTO_LOGIN = 0x001F,
    MLX5_OBJ_TYPE_FLOW_SAMPLER = 0x0020,
    MLX5_OBJ_TYPE_ASO_FLOW_METER = 0x0024,
    MLX5_OBJ_TYPE_ASO_FIRST_HIT = 0x0025,
    MLX5_OBJ_TYPE_SCHEDULING_ELEMENT = 0x0026,
    MLX5_OBJ_TYPE_RESERVED_QPN = 0x002C,
    MLX5_OBJ_TYPE_ASO_CT = 0x0031,
    MLX5_OBJ_TYPE_AV_QP_MAPPING = 0x003A,
};

struct mlx5_ifc_general_obj_in_cmd_hdr_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 obj_type[0x10];

    u8 obj_id[0x20];

    u8 reserved_at_60[0x3];
    u8 log_obj_range[0x5];
    u8 reserved_at_68[0x18];
};

struct mlx5_ifc_general_obj_out_cmd_hdr_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 obj_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_flow_meter_bits {
    u8 modify_field_select[0x40];

    u8 active[0x1];
    u8 reserved_at_41[0x3];
    u8 return_reg_id[0x4];
    u8 table_type[0x8];
    u8 reserved_at_50[0x10];

    u8 reserved_at_60[0x8];
    u8 destination_table_id[0x18];

    u8 reserved_at_80[0x80];

    u8 flow_meter_params[0x100];

    u8 reserved_at_180[0x180];

    u8 sw_steering_icm_address_rx[0x40];
    u8 sw_steering_icm_address_tx[0x40];
};

struct mlx5_ifc_create_flow_meter_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_flow_meter_bits meter;
};

struct mlx5_ifc_query_flow_meter_out_bits {
    struct mlx5_ifc_general_obj_out_cmd_hdr_bits hdr;
    struct mlx5_ifc_flow_meter_bits obj;
};

struct mlx5_ifc_flow_sampler_bits {
    u8 modify_field_select[0x40];

    u8 table_type[0x8];
    u8 level[0x8];
    u8 reserved_at_50[0xf];
    u8 ignore_flow_level[0x1];

    u8 sample_ratio[0x20];

    u8 reserved_at_80[0x8];
    u8 sample_table_id[0x18];

    u8 reserved_at_a0[0x8];
    u8 default_table_id[0x18];

    u8 sw_steering_icm_address_rx[0x40];
    u8 sw_steering_icm_address_tx[0x40];
};

struct mlx5_ifc_create_flow_sampler_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_flow_sampler_bits sampler;
};

struct mlx5_ifc_query_flow_sampler_out_bits {
    struct mlx5_ifc_general_obj_out_cmd_hdr_bits hdr;
    struct mlx5_ifc_flow_sampler_bits obj;
};

struct mlx5_ifc_definer_bits {
    u8 modify_field_select[0x40];

    u8 reserved_at_40[0x40];

    u8 reserved_at_80[0x10];
    u8 format_id[0x10];

    u8 reserved_at_60[0x160];

    u8 ctrl[0xA0];
    u8 match_mask_dw_11_8[0x60];
    u8 match_mask_dw_7_0[0x100];
};

struct mlx5_ifc_create_definer_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_definer_bits definer;
};

struct mlx5_ifc_esw_vport_context_bits {
    u8 reserved_at_0[0x3];
    u8 vport_svlan_strip[0x1];
    u8 vport_cvlan_strip[0x1];
    u8 vport_svlan_insert[0x1];
    u8 vport_cvlan_insert[0x2];
    u8 reserved_at_8[0x18];

    u8 reserved_at_20[0x20];

    u8 svlan_cfi[0x1];
    u8 svlan_pcp[0x3];
    u8 svlan_id[0xc];
    u8 cvlan_cfi[0x1];
    u8 cvlan_pcp[0x3];
    u8 cvlan_id[0xc];

    u8 reserved_at_40[0x720];
    u8 sw_steering_vport_icm_address_rx[0x40];
    u8 sw_steering_vport_icm_address_tx[0x40];
};

struct mlx5_ifc_query_esw_vport_context_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_esw_vport_context_bits esw_vport_context;
};

struct mlx5_ifc_query_esw_vport_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_nic_vport_context_bits {
    u8 reserved_at_0[0x1f];
    u8 roce_en[0x1];

    u8 reserved_at_20[0x7e0];
};

struct mlx5_ifc_query_nic_vport_context_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_nic_vport_context_bits nic_vport_context;
};

struct mlx5_ifc_query_nic_vport_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

enum {
    MLX5_QPC_ST_RC = 0x0,
};

enum {
    MLX5_QPC_PM_STATE_MIGRATED = 0x3,
};

struct mlx5_ifc_ud_av_bits {
    u8 reserved_at_0[0x60];

    u8 reserved_at_60[0x4];
    u8 sl_or_eth_prio[0x4];
    u8 reserved_at_68[0x18];

    u8 reserved_at_80[0x60];

    u8 reserved_at_e0[0x4];
    u8 src_addr_index[0x8];
    u8 reserved_at_ec[0x14];

    u8 rgid_or_rip[16][0x8];
};

struct mlx5_ifc_ads_bits {
    u8 fl[0x1];
    u8 free_ar[0x1];
    u8 reserved_at_2[0xe];
    u8 pkey_index[0x10];

    u8 reserved_at_20[0x8];
    u8 grh[0x1];
    u8 mlid[0x7];
    u8 rlid[0x10];

    u8 ack_timeout[0x5];
    u8 reserved_at_45[0x3];
    u8 src_addr_index[0x8];
    u8 reserved_at_50[0x4];
    u8 stat_rate[0x4];
    u8 hop_limit[0x8];

    u8 reserved_at_60[0x4];
    u8 tclass[0x8];
    u8 flow_label[0x14];

    u8 rgid_rip[16][0x8];

    u8 reserved_at_100[0x4];
    u8 f_dscp[0x1];
    u8 f_ecn[0x1];
    u8 reserved_at_106[0x1];
    u8 f_eth_prio[0x1];
    u8 ecn[0x2];
    u8 dscp[0x6];
    u8 udp_sport[0x10];

    u8 dei_cfi[0x1];
    u8 eth_prio[0x3];
    u8 sl[0x4];
    u8 vhca_port_num[0x8];
    u8 rmac_47_32[0x10];

    u8 rmac_31_0[0x20];
};

enum {
    MLX5_QPC_STATE_SQDRAINED = 0x5,
};

enum {
    MLX5_QPC_TIMESTAMP_FORMAT_FREE_RUNNING = 0x0,
    MLX5_QPC_TIMESTAMP_FORMAT_DEFAULT = 0x1,
    MLX5_QPC_TIMESTAMP_FORMAT_REAL_TIME = 0x2,
};

struct mlx5_ifc_qpc_bits {
    u8 state[0x4];
    u8 lag_tx_port_affinity[0x4];
    u8 st[0x8];
    u8 reserved_at_10[0x2];
    u8 isolate_vl_tc[0x1];
    u8 pm_state[0x2];
    u8 reserved_at_15[0x1];
    u8 req_e2e_credit_mode[0x2];
    u8 offload_type[0x4];
    u8 end_padding_mode[0x2];
    u8 reserved_at_1e[0x2];

    u8 wq_signature[0x1];
    u8 block_lb_mc[0x1];
    u8 atomic_like_write_en[0x1];
    u8 latency_sensitive[0x1];
    u8 reserved_at_24[0x1];
    u8 drain_sigerr[0x1];
    u8 reserved_at_26[0x2];
    u8 pd[0x18];

    u8 mtu[0x3];
    u8 log_msg_max[0x5];
    u8 reserved_at_48[0x1];
    u8 log_rq_size[0x4];
    u8 log_rq_stride[0x3];
    u8 no_sq[0x1];
    u8 log_sq_size[0x4];
    u8 reserved_at_55[0x3];
    u8 ts_format[0x2];
    u8 data_in_order[0x1];
    u8 rlky[0x1];
    u8 ulp_stateless_offload_mode[0x4];

    u8 counter_set_id[0x8];
    u8 uar_page[0x18];

    u8 reserved_at_80[0x8];
    u8 user_index[0x18];

    u8 reserved_at_a0[0x3];
    u8 log_page_size[0x5];
    u8 remote_qpn[0x18];

    struct mlx5_ifc_ads_bits primary_address_path;

    struct mlx5_ifc_ads_bits secondary_address_path;

    u8 log_ack_req_freq[0x4];
    u8 reserved_at_384[0x4];
    u8 log_sra_max[0x3];
    u8 reserved_at_38b[0x2];
    u8 retry_count[0x3];
    u8 rnr_retry[0x3];
    u8 reserved_at_393[0x1];
    u8 fre[0x1];
    u8 cur_rnr_retry[0x3];
    u8 cur_retry_count[0x3];
    u8 reserved_at_39b[0x5];

    u8 reserved_at_3a0[0x20];

    u8 reserved_at_3c0[0x8];
    u8 next_send_psn[0x18];

    u8 reserved_at_3e0[0x8];
    u8 cqn_snd[0x18];

    u8 reserved_at_400[0x8];
    u8 deth_sqpn[0x18];

    u8 reserved_at_420[0x20];

    u8 reserved_at_440[0x8];
    u8 last_acked_psn[0x18];

    u8 reserved_at_460[0x8];
    u8 ssn[0x18];

    u8 reserved_at_480[0x8];
    u8 log_rra_max[0x3];
    u8 reserved_at_48b[0x1];
    u8 atomic_mode[0x4];
    u8 rre[0x1];
    u8 rwe[0x1];
    u8 rae[0x1];
    u8 reserved_at_493[0x1];
    u8 page_offset[0x6];
    u8 reserved_at_49a[0x3];
    u8 cd_slave_receive[0x1];
    u8 cd_slave_send[0x1];
    u8 cd_master[0x1];

    u8 reserved_at_4a0[0x3];
    u8 min_rnr_nak[0x5];
    u8 next_rcv_psn[0x18];

    u8 reserved_at_4c0[0x8];
    u8 xrcd[0x18];

    u8 reserved_at_4e0[0x8];
    u8 cqn_rcv[0x18];

    u8 dbr_addr[0x40];

    u8 q_key[0x20];

    u8 reserved_at_560[0x5];
    u8 rq_type[0x3];
    u8 srqn_rmpn_xrqn[0x18];

    u8 reserved_at_580[0x8];
    u8 rmsn[0x18];

    u8 hw_sq_wqebb_counter[0x10];
    u8 sw_sq_wqebb_counter[0x10];

    u8 hw_rq_counter[0x20];

    u8 sw_rq_counter[0x20];

    u8 reserved_at_600[0x20];

    u8 reserved_at_620[0xf];
    u8 cgs[0x1];
    u8 cs_req[0x8];
    u8 cs_res[0x8];

    u8 dc_access_key[0x40];

    u8 reserved_at_680[0x3];
    u8 dbr_umem_valid[0x1];

    u8 reserved_at_684[0x9c];

    u8 dbr_umem_id[0x20];
};

struct mlx5_ifc_qpc_ext_bits {
    u8 reserved_at_0[0x2];
    u8 mmo[0x1];
    u8 reserved_at_3[0xd];
    u8 dci_stream_channel_id[0x10];

    u8 qos_queue_group_id_requester[0x20];

    u8 qos_queue_group_id_responder[0x20];

    u8 reserved_at_60[0x5a0];
};

struct mlx5_ifc_create_tir_out_bits {
    u8 status[0x8];
    u8 icm_address_63_40[0x18];

    u8 syndrome[0x20];

    u8 icm_address_39_32[0x8];
    u8 tirn[0x18];

    u8 icm_address_31_0[0x20];
};

struct mlx5_ifc_destroy_tir_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 tirn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 wq_umem_offset[0x40];
    u8 wq_umem_id[0x20];

    u8 wq_umem_valid[0x1];
    u8 reserved_at_861[0x1f];

    u8 pas[0][0x40];
};

struct mlx5_ifc_destroy_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];
};

enum mlx5_qpc_opt_mask_32 {
    MLX5_QPC_OPT_MASK_32_DCI_STREAM_CHANNEL_ID = 1 << 0,
    MLX5_QPC_OPT_MASK_32_QOS_QUEUE_GROUP_ID = 1 << 1,
    MLX5_QPC_OPT_MASK_32_UDP_SPORT = 1 << 2,
    MLX5_QPC_OPT_MASK_32_INIT2INIT_MMO = 1 << 3,
};

enum mlx5_qpc_opt_mask {
    MLX5_QPC_OPT_MASK_INIT2INIT_DRAIN_SIGERR = 1 << 11,
    MLX5_QPC_OPT_MASK_RTS2RTS_LAG_TX_PORT_AFFINITY = 1 << 15,
};

struct mlx5_ifc_init2init_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_init2init_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 qpc_ext[0x1];
    u8 reserved_at_41[0x7];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x40];

    u8 opt_param_mask_95_32[0x40];

    struct mlx5_ifc_qpc_ext_bits qpc_data_ext;
};

struct mlx5_ifc_init2rtr_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_init2rtr_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x80];
};

struct mlx5_ifc_rtr2rts_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_rtr2rts_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x80];
};

struct mlx5_ifc_rst2init_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_rst2init_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x80];
};

struct mlx5_ifc_rts2rts_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_rts2rts_qp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 qpc_ext[0x1];
    u8 reserved_at_41[0x7];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x40];

    u8 opt_param_mask_95_32[0x40];

    struct mlx5_ifc_qpc_ext_bits qpc_data_ext;
};

struct mlx5_ifc_query_qp_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    u8 opt_param_mask[0x20];

    u8 reserved_at_a0[0x20];

    struct mlx5_ifc_qpc_bits qpc;

    u8 reserved_at_800[0x80];

    u8 pas[0][0x40];
};

struct mlx5_ifc_query_qp_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_query_dct_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_dctc_bits dctc;
};

struct mlx5_ifc_query_dct_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 dctn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_tisc_bits {
    u8 strict_lag_tx_port_affinity[0x1];
    u8 tls_en[0x1];
    u8 reserved_at_2[0x2];
    u8 lag_tx_port_affinity[0x04];

    u8 reserved_at_8[0x4];
    u8 prio[0x4];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x100];

    u8 reserved_at_120[0x8];
    u8 transport_domain[0x18];

    u8 reserved_at_140[0x8];
    u8 underlay_qpn[0x18];

    u8 reserved_at_160[0x8];
    u8 pd[0x18];

    u8 reserved_at_180[0x380];
};

struct mlx5_ifc_query_tis_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_tisc_bits tis_context;
};

struct mlx5_ifc_query_tis_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 tisn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_lagc_bits {
    u8 reserved_at_0[0x1d];
    u8 lag_state[0x3];

    u8 reserved_at_20[0x14];
    u8 tx_remap_affinity_2[0x4];
    u8 reserved_at_38[0x4];
    u8 tx_remap_affinity_1[0x4];
};

struct mlx5_ifc_query_lag_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    struct mlx5_ifc_lagc_bits ctx;
};

struct mlx5_ifc_query_lag_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_av_qp_mapping_bits {
    u8 modify_field_select[0x40];

    u8 reserved_at_40[0x20];

    u8 qpn[0x20];

    struct mlx5_ifc_ud_av_bits remote_address_vector;
};

struct mlx5_ifc_create_av_qp_mapping_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_av_qp_mapping_bits mapping;
};

struct mlx5_ifc_query_av_qp_mapping_out_bits {
    struct mlx5_ifc_general_obj_out_cmd_hdr_bits hdr;
    struct mlx5_ifc_av_qp_mapping_bits obj;
};

struct mlx5_ifc_modify_tis_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_modify_tis_bitmask_bits {
    u8 reserved_at_0[0x20];

    u8 reserved_at_20[0x1d];
    u8 lag_tx_port_affinity[0x1];
    u8 strict_lag_tx_port_affinity[0x1];
    u8 prio[0x1];
};

struct mlx5_ifc_modify_tis_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 tisn[0x18];

    u8 reserved_at_60[0x20];

    struct mlx5_ifc_modify_tis_bitmask_bits bitmask;

    u8 reserved_at_c0[0x40];

    struct mlx5_ifc_tisc_bits ctx;
};

enum roce_version {
    MLX5_ROCE_VERSION_1 = 0,
    MLX5_ROCE_VERSION_2 = 2,
};

struct mlx5_ifc_roce_addr_layout_bits {
    u8 source_l3_address[16][0x8];

    u8 reserved_at_80[0x3];
    u8 vlan_valid[0x1];
    u8 vlan_id[0xc];
    u8 source_mac_47_32[0x10];

    u8 source_mac_31_0[0x20];

    u8 reserved_at_c0[0x14];
    u8 roce_l3_type[0x4];
    u8 roce_version[0x8];

    u8 reserved_at_e0[0x20];
};

struct mlx5_ifc_query_roce_address_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_roce_addr_layout_bits roce_address;
};

struct mlx5_ifc_query_roce_address_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 roce_address_index[0x10];
    u8 reserved_at_50[0xc];
    u8 vhca_port_num[0x4];

    u8 reserved_at_60[0x20];
};

/* Both HW set and HW add share the same HW format with different opcodes */
struct mlx5_ifc_dr_action_hw_set_bits {
    u8 opcode[0x8];
    u8 destination_field_code[0x8];
    u8 reserved_at_10[0x2];
    u8 destination_left_shifter[0x6];
    u8 reserved_at_18[0x3];
    u8 destination_length[0x5];

    u8 inline_data[0x20];
};

struct mlx5_ifc_dr_action_hw_copy_bits {
    u8 opcode[0x8];
    u8 destination_field_code[0x8];
    u8 reserved_at_10[0x2];
    u8 destination_left_shifter[0x6];
    u8 reserved_at_18[0x2];
    u8 destination_length[0x6];

    u8 reserved_at_20[0x8];
    u8 source_field_code[0x8];
    u8 reserved_at_30[0x2];
    u8 source_left_shifter[0x6];
    u8 reserved_at_38[0x8];
};

struct mlx5_ifc_host_params_context_bits {
    u8 host_number[0x8];
    u8 reserved_at_8[0x6];
    u8 host_pf_vhca_id_valid[0x1];
    u8 host_pf_disabled[0x1];
    u8 host_num_of_vfs[0x10];

    u8 host_total_vfs[0x10];
    u8 host_pci_bus[0x10];

    u8 host_pf_vhca_id[0x10];
    u8 host_pci_device[0x10];

    u8 reserved_at_60[0x10];
    u8 host_pci_function[0x10];

    u8 reserved_at_80[0x180];
};

struct mlx5_ifc_query_esw_functions_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_query_esw_functions_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_host_params_context_bits host_params_context;

    u8 reserved_at_280[0x180];
    u8 host_sf_enable[0][0x40];
};

struct mlx5_ifc_create_flow_group_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 reserved_at_c0[0x1f40];
};

struct mlx5_ifc_create_flow_group_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 group_id[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_flow_group_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 group_id[0x20];

    u8 reserved_at_e0[0x120];
};

struct mlx5_ifc_dest_format_bits {
    u8 destination_type[0x8];
    u8 destination_id[0x18];

    u8 reserved_at_20[0x1];
    u8 packet_reformat[0x1];
    u8 reserved_at_22[0x1e];
};

struct mlx5_ifc_extended_dest_format_bits {
    struct mlx5_ifc_dest_format_bits destination_entry;

    u8 packet_reformat_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_flow_counter_list_bits {
    u8 flow_counter_id[0x20];

    u8 reserved_at_20[0x20];
};

union mlx5_ifc_dest_format_flow_counter_list_auto_bits {
    struct mlx5_ifc_dest_format_bits dest_format;
    struct mlx5_ifc_flow_counter_list_bits flow_counter_list;
    u8 reserved_at_0[0x40];
};

struct mlx5_ifc_flow_context_bits {
    u8 reserved_at_00[0x20];

    u8 group_id[0x20];

    u8 reserved_at_40[0x8];
    u8 flow_tag[0x18];

    u8 reserved_at_60[0x10];
    u8 action[0x10];

    u8 extended_destination[0x1];
    u8 reserved_at_81[0x7];
    u8 destination_list_size[0x18];

    u8 reserved_at_a0[0x8];
    u8 flow_counter_list_size[0x18];

    u8 reserved_at_c0[0x1740];

    union mlx5_ifc_dest_format_flow_counter_list_auto_bits destination[0];
};

struct mlx5_ifc_set_fte_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 reserved_at_c0[0x40];
    u8 flow_index[0x20];

    u8 reserved_at_120[0xe0];
    struct mlx5_ifc_flow_context_bits flow_context;
};

struct mlx5_ifc_set_fte_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

enum dr_devx_flow_dest_type {
    MLX5_FLOW_DEST_TYPE_VPORT = 0x0,
    MLX5_FLOW_DEST_TYPE_FT = 0x1,
    MLX5_FLOW_DEST_TYPE_TIR = 0x2,

    MLX5_FLOW_DEST_TYPE_COUNTER = 0x100,
};

enum {
    MLX5_FLOW_CONTEXT_ACTION_FWD_DEST = 0x4,
    MLX5_FLOW_CONTEXT_ACTION_COUNT = 0x8,
};

enum {
    MLX5_QPC_PAGE_OFFSET_QUANTA = 64,
};

enum {
    MLX5_ASO_FIRST_HIT_NUM_PER_OBJ = 512,
    MLX5_ASO_FLOW_METER_NUM_PER_OBJ = 2,
    MLX5_ASO_CT_NUM_PER_OBJ = 1,
};

enum mlx5_sched_hierarchy_type {
    MLX5_SCHED_HIERARCHY_NIC = 3,
};

enum mlx5_sched_elem_type {
    MLX5_SCHED_ELEM_TYPE_TSAR = 0x0,
    MLX5_SCHED_ELEM_TYPE_VPORT = 0x1,
    MLX5_SCHED_ELEM_TYPE_VPORT_TC = 0x2,
    MLX5_SCHED_ELEM_TYPE_PARA_VPORT_TC = 0x3,
    MLX5_SCHED_ELEM_TYPE_QUEUE_GROUP = 0x4,
};

enum mlx5_sched_tsar_type {
    MLX5_SCHED_TSAR_TYPE_DWRR = 0x0,
    MLX5_SCHED_TSAR_TYPE_ROUND_ROBIN = 0x1,
    MLX5_SCHED_TSAR_TYPE_ETS = 0x2,
};

struct mlx5_ifc_sched_elem_attr_tsar_bits {
    u8 reserved_at_0[0x8];
    u8 tsar_type[0x8];
    u8 reserved_at_10[0x10];
};

union mlx5_ifc_sched_elem_attr_bits {
    struct mlx5_ifc_sched_elem_attr_tsar_bits tsar;
};

struct mlx5_ifc_sched_context_bits {
    u8 element_type[0x8];
    u8 reserved_at_8[0x18];

    union mlx5_ifc_sched_elem_attr_bits sched_elem_attr;

    u8 parent_element_id[0x20];

    u8 reserved_at_60[0x40];

    u8 bw_share[0x20];

    u8 max_average_bw[0x20];

    u8 reserved_at_e0[0x120];
};

struct mlx5_ifc_sched_elem_bits {
    u8 modify_field_select[0x40];

    u8 scheduling_hierarchy[0x8];
    u8 reserved_at_48[0x18];

    u8 reserved_at_60[0xa0];

    struct mlx5_ifc_sched_context_bits sched_context;

    u8 reserved_at_300[0x100];
};

struct mlx5_ifc_create_sched_elem_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_sched_elem_bits sched_elem;
};

struct mlx5_ifc_create_modify_elem_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_sched_elem_bits sched_elem;
};

enum {
    MLX5_SQC_STATE_RDY = 0x1,
};

struct mlx5_ifc_sqc_bits {
    u8 reserved_at_0[0x8];
    u8 state[0x4];
    u8 reserved_at_c[0x14];

    u8 reserved_at_20[0xe0];

    u8 reserved_at_100[0x10];
    u8 qos_queue_group_id[0x10];

    u8 reserved_at_120[0x660];
};

enum {
    MLX5_MODIFY_SQ_BITMASK_QOS_QUEUE_GROUP_ID = 1 << 2,
};

struct mlx5_ifc_modify_sq_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_modify_sq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 sq_state[0x4];
    u8 reserved_at_44[0x4];
    u8 sqn[0x18];

    u8 reserved_at_60[0x20];

    u8 modify_bitmask[0x40];

    u8 reserved_at_c0[0x40];

    struct mlx5_ifc_sqc_bits sq_context;
};

struct mlx5_ifc_reserved_qpn_bits {
    u8 reserved_at_0[0x80];
};

struct mlx5_ifc_create_reserved_qpn_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_reserved_qpn_bits rqpns;
};

struct mlx5_ifc_create_psv_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    u8 reserved_at_80[0x8];
    u8 psv0_index[0x18];

    u8 reserved_at_a0[0x8];
    u8 psv1_index[0x18];

    u8 reserved_at_c0[0x8];
    u8 psv2_index[0x18];

    u8 reserved_at_e0[0x8];
    u8 psv3_index[0x18];
};

struct mlx5_ifc_create_psv_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 num_psv[0x4];
    u8 reserved_at_44[0x4];
    u8 pd[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_psv_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 psvn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_mbox_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_mbox_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_enable_hca_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x10];
    u8 function_id[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_enable_hca_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x20];
};

struct mlx5_ifc_query_issi_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x10];
    u8 current_issi[0x10];

    u8 reserved_at_60[0xa0];

    u8 reserved_at_100[76][0x8];
    u8 supported_issi_dw0[0x20];
};

struct mlx5_ifc_query_issi_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_set_issi_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_set_issi_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x10];
    u8 current_issi[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_query_pages_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 embedded_cpu_function[0x01];
    u8 reserved_bits[0x0f];
    u8 function_id[0x10];

    u8 num_pages[0x20];
};

struct mlx5_ifc_query_pages_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x10];
    u8 function_id[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_manage_pages_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 output_num_entries[0x20];

    u8 reserved_at_60[0x20];

    u8 pas[][0x40];
};

struct mlx5_ifc_manage_pages_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 embedded_cpu_function[0x1];
    u8 reserved_at_41[0xf];
    u8 function_id[0x10];

    u8 input_num_entries[0x20];

    u8 pas[][0x40];
};

enum {
    MLX5_TEARDOWN_HCA_OUT_FORCE_STATE_FAIL = 0x1,
};

struct mlx5_ifc_teardown_hca_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x3f];

    u8 state[0x1];
};

enum {
    MLX5_TEARDOWN_HCA_IN_PROFILE_GRACEFUL_CLOSE = 0x0,
    MLX5_TEARDOWN_HCA_IN_PROFILE_PREPARE_FAST_TEARDOWN = 0x2,
};

struct mlx5_ifc_teardown_hca_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x10];
    u8 profile[0x10];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_init_hca_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_init_hca_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_access_register_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];

    u8 register_data[][0x20];
};

struct mlx5_ifc_access_register_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x10];
    u8 register_id[0x10];

    u8 argument[0x20];

    u8 register_data[][0x20];
};

struct mlx5_ifc_modify_nic_vport_context_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_modify_nic_vport_field_select_bits {
    u8 reserved_at_0[0x12];
    u8 affiliation[0x1];
    u8 reserved_at_13[0x1];
    u8 disable_uc_local_lb[0x1];
    u8 disable_mc_local_lb[0x1];
    u8 node_guid[0x1];
    u8 port_guid[0x1];
    u8 min_inline[0x1];
    u8 mtu[0x1];
    u8 change_event[0x1];
    u8 promisc[0x1];
    u8 permanent_address[0x1];
    u8 addresses_list[0x1];
    u8 roce_en[0x1];
    u8 reserved_at_1f[0x1];
};

struct mlx5_ifc_modify_nic_vport_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    struct mlx5_ifc_modify_nic_vport_field_select_bits field_select;

    u8 reserved_at_80[0x780];

    struct mlx5_ifc_nic_vport_context_bits nic_vport_context;
};

struct mlx5_ifc_set_hca_cap_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_set_hca_cap_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 other_function[0x1];
    u8 reserved_at_41[0xf];
    u8 function_id[0x10];

    u8 reserved_at_60[0x20];

    union mlx5_ifc_hca_cap_union_bits capability;
};

struct mlx5_ifc_alloc_uar_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 uar[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_uar_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_dealloc_uar_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_dealloc_uar_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 uar[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_eqc_bits {
    u8 status[0x4];
    u8 reserved_at_4[0x9];
    u8 ec[0x1];
    u8 oi[0x1];
    u8 reserved_at_f[0x5];
    u8 st[0x4];
    u8 reserved_at_18[0x8];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x14];
    u8 page_offset[0x6];
    u8 reserved_at_5a[0x6];

    u8 reserved_at_60[0x3];
    u8 log_eq_size[0x5];
    u8 uar_page[0x18];

    u8 reserved_at_80[0x20];

    u8 reserved_at_a0[0x18];
    u8 intr[0x8];

    u8 reserved_at_c0[0x3];
    u8 log_page_size[0x5];
    u8 reserved_at_c8[0x18];

    u8 reserved_at_e0[0x60];

    u8 reserved_at_140[0x8];
    u8 consumer_counter[0x18];

    u8 reserved_at_160[0x8];
    u8 producer_counter[0x18];

    u8 reserved_at_180[0x80];
};

struct mlx5_ifc_create_eq_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x18];
    u8 eq_number[0x8];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_eq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_eqc_bits eq_context_entry;

    u8 reserved_at_280[0x40];

    u8 event_bitmask[4][0x40];

    u8 reserved_at_3c0[0x4c0];

    u8 pas[][0x40];
};

struct mlx5_ifc_destroy_eq_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_destroy_eq_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x18];
    u8 eq_number[0x8];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_pd_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 pd[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_pd_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_dealloc_pd_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_dealloc_pd_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 pd[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_mtt_bits {
    u8 ptag_63_32[0x20];

    u8 ptag_31_8[0x18];
    u8 reserved_at_38[0x6];
    u8 wr_en[0x1];
    u8 rd_en[0x1];
};

struct mlx5_ifc_umem_bits {
    u8 reserved_at_0[0x80];

    u8 reserved_at_80[0x1b];
    u8 log_page_size[0x5];

    u8 page_offset[0x20];

    u8 num_of_mtt[0x40];

    struct mlx5_ifc_mtt_bits mtt[];
};

struct mlx5_ifc_create_umem_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x40];

    struct mlx5_ifc_umem_bits umem;
};

struct mlx5_ifc_create_umem_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x8];
    u8 umem_id[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_umem_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x10];
    u8 op_mod[0x10];

    u8 reserved_at_40[0x8];
    u8 umem_id[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_umem_out_bits {
    u8 status[0x8];
    u8 reserved_at_8[0x18];

    u8 syndrome[0x20];

    u8 reserved_at_40[0x40];
};

struct mlx5_ifc_delete_fte_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 other_vport[0x1];
    u8 reserved_at_41[0xf];
    u8 vport_number[0x10];

    u8 reserved_at_60[0x20];

    u8 table_type[0x8];
    u8 reserved_at_88[0x18];

    u8 reserved_at_a0[0x8];
    u8 table_id[0x18];

    u8 reserved_at_c0[0x40];

    u8 flow_index[0x20];

    u8 reserved_at_120[0xe0];
};

struct mlx5_ifc_create_cq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 cqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_cq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 cqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_transport_domain_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 transport_domain[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_transport_domain_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 transport_domain[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_rmp_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 rmpn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_rmp_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 rmpn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_sq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 sqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_sq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 sqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_rq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 rqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_rq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 rqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_rqt_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 rqtn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_rqt_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 rqtn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_tis_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 tisn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_tis_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 tisn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_q_counter_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x18];
    u8 counter_set_id[0x8];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_q_counter_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x18];
    u8 counter_set_id[0x8];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_alloc_modify_header_context_out_bits {
    u8 reserved_at_0[0x40];

    u8 modify_header_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_modify_header_context_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 modify_header_id[0x20];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_scheduling_element_out_bits {
    u8 reserved_at_0[0x80];

    u8 scheduling_element_id[0x20];

    u8 reserved_at_a0[0x160];
};

struct mlx5_ifc_create_scheduling_element_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 scheduling_hierarchy[0x8];
    u8 reserved_at_48[0x18];

    u8 reserved_at_60[0x3a0];
};

struct mlx5_ifc_destroy_scheduling_element_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x20];

    u8 scheduling_hierarchy[0x8];
    u8 reserved_at_48[0x18];

    u8 scheduling_element_id[0x20];

    u8 reserved_at_80[0x180];
};

struct mlx5_ifc_add_vxlan_udp_dport_in_bits {
    u8 reserved_at_0[0x60];

    u8 reserved_at_60[0x10];
    u8 vxlan_udp_port[0x10];
};

struct mlx5_ifc_delete_vxlan_udp_dport_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x40];

    u8 reserved_at_60[0x10];
    u8 vxlan_udp_port[0x10];
};

struct mlx5_ifc_set_l2_table_entry_in_bits {
    u8 reserved_at_0[0xa0];

    u8 reserved_at_a0[0x8];
    u8 table_index[0x18];

    u8 reserved_at_c0[0x140];
};

struct mlx5_ifc_delete_l2_table_entry_in_bits {
    u8 opcode[0x10];
    u8 reserved_at_10[0x10];

    u8 reserved_at_20[0x80];

    u8 reserved_at_a0[0x8];
    u8 table_index[0x18];

    u8 reserved_at_c0[0x140];
};

struct mlx5_ifc_create_srq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 srqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_srq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 srqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_xrc_srq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 xrc_srqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_xrc_srq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 xrc_srqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_dct_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 dctn[0x18];

    u8 ece[0x20];
};

struct mlx5_ifc_destroy_dct_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 dctn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_create_xrq_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 xrqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_destroy_xrq_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 xrqn[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_attach_to_mcg_in_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 multicast_gid[16][0x8];
};

struct mlx5_ifc_detach_from_mcg_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 qpn[0x18];

    u8 reserved_at_60[0x20];

    u8 multicast_gid[16][0x8];
};

struct mlx5_ifc_alloc_xrcd_out_bits {
    u8 reserved_at_0[0x40];

    u8 reserved_at_40[0x8];
    u8 xrcd[0x18];

    u8 reserved_at_60[0x20];
};

struct mlx5_ifc_dealloc_xrcd_in_bits {
    u8 opcode[0x10];
    u8 uid[0x10];

    u8 reserved_at_20[0x20];

    u8 reserved_at_40[0x8];
    u8 xrcd[0x18];

    u8 reserved_at_60[0x20];
};

enum {
    MLX5_CRYPTO_LOGIN_OBJ_STATE_VALID = 0x0,
    MLX5_CRYPTO_LOGIN_OBJ_STATE_INVALID = 0x1,
};

struct mlx5_ifc_crypto_login_obj_bits {
    u8 modify_field_select[0x40];

    u8 reserved_at_40[0x40];

    u8 reserved_at_80[0x4];
    u8 state[0x4];
    u8 credential_pointer[0x18];

    u8 reserved_at_a0[0x8];
    u8 session_import_kek_ptr[0x18];

    u8 reserved_at_c0[0x140];

    u8 credential[12][0x20];

    u8 reserved_at_380[0x480];
};

struct mlx5_ifc_create_crypto_login_obj_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_crypto_login_obj_bits login_obj;
};

struct mlx5_ifc_query_crypto_login_obj_out_bits {
    struct mlx5_ifc_general_obj_out_cmd_hdr_bits hdr;
    struct mlx5_ifc_crypto_login_obj_bits obj;
};

enum {
    MLX5_ENCRYPTION_KEY_OBJ_STATE_READY = 0x0,
    MLX5_ENCRYPTION_KEY_OBJ_STATE_ERROR = 0x1,
};

enum {
    MLX5_ENCRYPTION_KEY_OBJ_KEY_SIZE_SIZE_128 = 0x0,
    MLX5_ENCRYPTION_KEY_OBJ_KEY_SIZE_SIZE_256 = 0x1,
};

enum {
    MLX5_ENCRYPTION_KEY_OBJ_KEY_PURPOSE_AES_XTS = 0x3,
};

struct mlx5_ifc_encryption_key_obj_bits {
    u8 modify_field_select[0x40];

    u8 state[0x8];
    u8 reserved_at_48[0xc];
    u8 key_size[0x4];
    u8 has_keytag[0x1];
    u8 reserved_at_59[0x3];
    u8 key_purpose[0x4];

    u8 reserved_at_60[0x8];
    u8 pd[0x18];

    u8 reserved_at_80[0x100];

    u8 opaque[0x40];

    u8 reserved_at_1c0[0x40];

    u8 key[32][0x20];

    u8 reserved_at_600[0x200];
};

struct mlx5_ifc_create_encryption_key_obj_in_bits {
    struct mlx5_ifc_general_obj_in_cmd_hdr_bits hdr;
    struct mlx5_ifc_encryption_key_obj_bits key_obj;
};

struct mlx5_ifc_query_encryption_key_obj_out_bits {
    struct mlx5_ifc_general_obj_out_cmd_hdr_bits hdr;
    struct mlx5_ifc_encryption_key_obj_bits obj;
};

enum {
    MLX5_ENCRYPTION_ORDER_ENCRYPTED_WIRE_SIGNATURE = 0x0,
    MLX5_ENCRYPTION_ORDER_ENCRYPTED_MEMORY_SIGNATURE = 0x1,
    MLX5_ENCRYPTION_ORDER_ENCRYPTED_RAW_WIRE = 0x2,
    MLX5_ENCRYPTION_ORDER_ENCRYPTED_RAW_MEMORY = 0x3,
};

enum {
    MLX5_ENCRYPTION_STANDARD_AES_XTS = 0x0,
};

#endif /* MLX5_IFC_H */
