// mooncake_worker.mu - MUSA build entry point for mooncake_worker kernel code.
// This file is compiled by mcc (MUSA compiler) when MOONCAKE_EP_USE_MUSA is set.
// It includes the kernel-only source to avoid mcc crashes on torch headers.

#define MOONCAKE_EP_USE_MUSA 1
#include "mooncake_worker_kernels.cu"
