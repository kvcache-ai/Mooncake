#pragma once

#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>

namespace mooncake::tent::device::ibgda {

static constexpr size_t kBlueFlameSize = 256;

struct IbGdaWqDoorbellRecord {
    __be32 rcv_counter;
    __be32 send_counter;
};

struct IbGdaWqebb {
    uint64_t qwords[8];
};

struct IbGdaRdmaWriteWqe {
    mlx5_wqe_ctrl_seg ctrl;
    mlx5_wqe_raddr_seg raddr;
    mlx5_wqe_data_seg data;
};

struct IbGdaRdmaAtomicWqe {
    mlx5_wqe_ctrl_seg ctrl;
    mlx5_wqe_raddr_seg raddr;
    mlx5_wqe_atomic_seg atomic;
    mlx5_wqe_data_seg data;
};

struct IbGdaQpDevCtx {
    uint32_t qpn;
    uint32_t wqeid_mask;
    uint32_t mutex;
    IbGdaWqebb* wq;
    mlx5_cqe64* cq;
    IbGdaWqDoorbellRecord* dbr;
    char* bf;
    uint32_t bf_offset;
    uint16_t wq_head;
    uint16_t wq_tail;
};

static __device__ __forceinline__ uint16_t bswap16(uint16_t x) {
    return __byte_perm(x, x, 0x2301);
}

static __device__ __forceinline__ uint32_t bswap32(uint32_t x) {
    return __byte_perm(x, x, 0x0123);
}

static __device__ __forceinline__ uint64_t bswap64(uint64_t x) {
    uint32_t hi = static_cast<uint32_t>(x >> 32);
    uint32_t lo = static_cast<uint32_t>(x);
    hi = __byte_perm(hi, hi, 0x0123);
    lo = __byte_perm(lo, lo, 0x0123);
    return (static_cast<uint64_t>(lo) << 32) | hi;
}

struct mlx5_wqe_atomic_add_32_seg {
    __be32 add_data;
    __be32 field_boundary;
    __be64 compare;
};

}  // namespace mooncake::tent::device::ibgda
