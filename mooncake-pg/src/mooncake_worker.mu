// mooncake_worker.mu - MUSA build entry point for mooncake_worker kernel code.
// This file is compiled by mcc (MUSA compiler) when MOONCAKE_EP_USE_MUSA is set.
// It includes the same .cu source with the MUSA macro pre-defined so that
// #ifdef guards inside mooncake_worker.cu activate the MUSA code paths.

#define MOONCAKE_EP_USE_MUSA 1
#include "mooncake_worker.cu"
