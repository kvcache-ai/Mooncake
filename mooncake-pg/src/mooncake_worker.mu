// mooncake_worker.mu — MUSA build entry point.
// Compiled by mcc. All kernel functions and launch wrappers live in
// mooncake_worker_kernels.cu (shared with CUDA's mooncake_worker.cu).
// Host code lives in mooncake_worker_host.cpp (shared with CUDA).

#define MOONCAKE_EP_USE_MUSA 1
#include "mooncake_worker_kernels.cu"
