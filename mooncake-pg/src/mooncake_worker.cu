// mooncake_worker.cu — CUDA build entry point.
// Compiled by nvcc. All kernel functions and launch wrappers live in
// mooncake_worker_kernels.cu (shared with MUSA's mooncake_worker.mu).
// Host code lives in mooncake_worker_host.cpp (shared with MUSA).

#include "mooncake_worker_kernels.cu"
