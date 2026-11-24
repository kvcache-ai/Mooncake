// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Test-only liburing surface. The production executor/driver is compiled
// unchanged against this header in one isolated deterministic executable.
// Keep UAPI structs and opcodes self-contained so the fixture compiles even on
// distros whose <linux/io_uring.h> predates IORING_OP_READ (Linux < 5.6).
#pragma once

#include <sys/uio.h>
#include <cstdint>
#include <cstring>

enum {
    IORING_OP_READ_FIXED = 4,
    IORING_OP_READ = 22,
};

enum {
    IO_URING_OP_SUPPORTED = 1U << 0,
};

struct io_uring_sqe {
    uint8_t opcode;
    uint8_t flags;
    int32_t fd;
    uint64_t off;
    uint64_t addr;
    uint32_t len;
    uint64_t user_data;
    uint16_t buf_index;
};

struct io_uring_cqe {
    uint64_t user_data;
    int32_t res;
    uint32_t flags;
};

struct io_uring_probe_op {
    uint8_t op;
    uint8_t resv;
    uint16_t flags;
    uint32_t resv2;
};

struct io_uring_probe {
    uint8_t last_op;
    uint8_t ops_len;
    uint16_t resv;
    uint32_t resv2[3];
    io_uring_probe_op ops[32];
};

struct io_uring {
    int ring_fd{-1};
    void* test_state{nullptr};
};

extern "C" {
int io_uring_queue_init(unsigned, io_uring*, unsigned);
void io_uring_queue_exit(io_uring*);
io_uring_probe* io_uring_get_probe_ring(io_uring*);
void io_uring_free_probe(io_uring_probe*);
io_uring_sqe* io_uring_get_sqe(io_uring*);
unsigned io_uring_sq_ready(const io_uring*);
unsigned io_uring_sq_space_left(const io_uring*);
int io_uring_submit(io_uring*);
int io_uring_peek_cqe(io_uring*, io_uring_cqe**);
void io_uring_cqe_seen(io_uring*, io_uring_cqe*);
int io_uring_register_buffers(io_uring*, const iovec*, unsigned);
int io_uring_unregister_buffers(io_uring*);
}

inline int io_uring_opcode_supported(const io_uring_probe* p, int op) {
    if (!p || op < 0 || op > p->last_op) return 0;
    return (p->ops[op].flags & IO_URING_OP_SUPPORTED) != 0;
}

inline void io_uring_prep_read(io_uring_sqe* sqe, int fd, void* buffer,
                               unsigned length, uint64_t offset) {
    std::memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = IORING_OP_READ;
    sqe->fd = fd;
    sqe->addr = reinterpret_cast<uintptr_t>(buffer);
    sqe->len = length;
    sqe->off = offset;
}
inline void io_uring_prep_read_fixed(io_uring_sqe* sqe, int fd, void* buffer,
                                     unsigned length, uint64_t offset,
                                     unsigned index) {
    io_uring_prep_read(sqe, fd, buffer, length, offset);
    sqe->opcode = IORING_OP_READ_FIXED;
    sqe->buf_index = index;
}
inline void io_uring_sqe_set_data64(io_uring_sqe* sqe, uint64_t value) {
    sqe->user_data = value;
}
inline uint64_t io_uring_cqe_get_data64(const io_uring_cqe* cqe) {
    return cqe->user_data;
}
